# server.py - Databento 실시간 NQ 가격 서버
# Railway.app에서 상시 실행
# Databento Live TCP 연결 유지 → 최신 가격 메모리 저장 → HTTP로 제공
# + 1분 캔들 히스토리 (24시간 보관)

import os
import json
import time
import threading
from collections import deque
from flask import Flask, jsonify, request
from flask_cors import CORS
import databento as db

app = Flask(__name__)
CORS(app)

# 최신 가격 저장소
latest_data = {
    'symbol': 'NQ',
    'price': None,
    'bid': None,
    'ask': None,
    'volume': 0,
    'timestamp': None,
    'source': 'databento-live',
    'connected': False,
    'last_update': None,
    'error': None
}

# ========== 1분 캔들 히스토리 ==========
# 최대 1440개 (24시간 × 60분)
candle_history = deque(maxlen=1440)
current_candle = None
candle_lock = threading.Lock()

def get_candle_time(ts_seconds):
    """타임스탬프를 1분 단위로 내림"""
    return (ts_seconds // 60) * 60

def update_candle(price, ts_seconds):
    """새 틱이 들어올 때 1분 캔들 업데이트 (스파이크 필터 포함)"""
    global current_candle
    
    candle_time = get_candle_time(ts_seconds)
    
    with candle_lock:
        # 현재 캔들 내에서 비정상 변동 체크
        if current_candle is not None and current_candle['time'] == candle_time:
            mid = (current_candle['high'] + current_candle['low']) / 2
            if abs(price - mid) > 30:
                return  # 캔들 내 30포인트 이상 스파이크 무시
        
        if current_candle is None or current_candle['time'] != candle_time:
            # 이전 캔들 저장
            if current_candle is not None:
                candle_history.append(current_candle.copy())
            
            # 새 캔들 시작
            current_candle = {
                'time': candle_time,
                'open': price,
                'high': price,
                'low': price,
                'close': price
            }
        else:
            # 기존 캔들 업데이트
            current_candle['high'] = max(current_candle['high'], price)
            current_candle['low'] = min(current_candle['low'], price)
            current_candle['close'] = price


def run_live_feed():
    """Databento Live API에 상시 연결하여 NQ 가격 수신"""
    global latest_data
    api_key = os.environ.get('DATABENTO_API_KEY')
    
    if not api_key:
        latest_data['error'] = 'DATABENTO_API_KEY not set'
        return
    
    while True:
        try:
            print("🔌 Databento Live 연결 시작...")
            client = db.Live(key=api_key)
            
            # NQ 연속 계약 구독 (mbp-1 = top of book, 가장 가벼운 스키마)
            client.subscribe(
                dataset='GLBX.MDP3',
                schema='mbp-1',
                symbols=['NQ.c.0'],
                stype_in='continuous',
            )
            
            latest_data['connected'] = True
            latest_data['error'] = None
            print("✅ Databento Live 연결 성공! NQ 데이터 수신 중...")
            
            for record in client:
                try:
                    price = None
                    bid = None
                    ask = None
                    
                    # === trade 가격 (가장 정확) ===
                    if hasattr(record, 'price') and record.price:
                        p = record.price / 1e9
                        if p > 1000:
                            price = round(p, 2)
                    
                    # === bid/ask에서 mid-price (trade 없을 때만) ===
                    if price is None and hasattr(record, 'levels') and len(record.levels) > 0:
                        level = record.levels[0]
                        raw_bid = level.bid_px
                        raw_ask = level.ask_px
                        
                        # fixed-point 변환 (1e-9)
                        b = raw_bid / 1e9 if raw_bid and raw_bid > 0 else None
                        a = raw_ask / 1e9 if raw_ask and raw_ask > 0 else None
                        
                        if b and b > 1000 and a and a > 1000:
                            spread = a - b
                            # 스프레드가 비정상적으로 크면 무시 (정상: 0.25~2.0)
                            if spread < 10:
                                bid = round(b, 2)
                                ask = round(a, 2)
                                price = round((b + a) / 2, 2)
                                latest_data['bid'] = bid
                                latest_data['ask'] = ask
                    
                    if price and price > 1000:
                        # === 스파이크 필터 ===
                        # 이전 가격 대비 50포인트(~0.2%) 이상 변동 시 무시
                        prev = latest_data.get('price')
                        if prev and abs(price - prev) > 50:
                            # 급변동 감지 - 스킵
                            print(f"⚠️ 스파이크 필터: {prev} → {price} (차이: {abs(price-prev):.1f}pt)")
                            continue
                        
                        latest_data['price'] = price
                        latest_data['timestamp'] = int(time.time() * 1000)
                        latest_data['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
                        
                        # volume
                        if hasattr(record, 'size'):
                            latest_data['volume'] = record.size
                        
                        # 캔들 업데이트
                        update_candle(price, int(time.time()))
                            
                except Exception as e:
                    print(f"⚠️ 레코드 파싱 에러: {e}")
                    continue
                    
        except Exception as e:
            print(f"❌ Databento 연결 에러: {e}")
            latest_data['connected'] = False
            latest_data['error'] = str(e)
            
            # 5초 후 재연결
            print("🔄 5초 후 재연결...")
            time.sleep(5)


@app.route('/')
def index():
    return jsonify({
        'service': 'CROWNY NQ Price Server',
        'status': 'running',
        'connected': latest_data['connected'],
        'candles': len(candle_history)
    })


@app.route('/api/market/live')
def get_live_price():
    return jsonify(latest_data)


@app.route('/api/market/candles')
def get_candles():
    """1분 캔들 히스토리 반환
    
    Query params:
        limit: 반환할 캔들 수 (기본 1440, 최대 1440)
    """
    limit = min(int(request.args.get('limit', 1440)), 1440)
    
    with candle_lock:
        # 완성된 캔들 + 현재 진행 중 캔들
        candles = list(candle_history)
        if current_candle is not None:
            candles.append(current_candle.copy())
    
    # 최신 limit개만
    if len(candles) > limit:
        candles = candles[-limit:]
    
    return jsonify({
        'candles': candles,
        'count': len(candles),
        'interval': '1m',
        'symbol': 'NQ'
    })


@app.route('/api/market/health')
def health():
    return jsonify({
        'status': 'ok',
        'connected': latest_data['connected'],
        'last_update': latest_data['last_update'],
        'candles_stored': len(candle_history),
        'error': latest_data['error']
    })


if __name__ == '__main__':
    # Databento 피드를 백그라운드 스레드에서 실행
    feed_thread = threading.Thread(target=run_live_feed, daemon=True)
    feed_thread.start()
    
    port = int(os.environ.get('PORT', 8080))
    print(f"🚀 서버 시작: 포트 {port}")
    app.run(host='0.0.0.0', port=port)
