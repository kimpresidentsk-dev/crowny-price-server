# server.py - Databento 실시간 NQ + MNQ 가격 서버
# Railway.app에서 상시 실행
# NQ + MNQ 개별 구독 → 심볼별 데이터 → HTTP API 제공

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

# ========== NQ / MNQ 개별 데이터 ==========
symbols_data = {
    'NQ': {
        'price': None, 'bid': None, 'ask': None, 'volume': 0,
        'timestamp': None, 'last_update': None
    },
    'MNQ': {
        'price': None, 'bid': None, 'ask': None, 'volume': 0,
        'timestamp': None, 'last_update': None
    }
}

feed_status = {'connected': False, 'error': None}

# instrument_id → symbol 매핑 (subscribe 후 자동 세팅)
iid_to_symbol = {}

# 1분 캔들 히스토리 (심볼별, 볼륨 포함)
candle_history = {'NQ': deque(maxlen=1440), 'MNQ': deque(maxlen=1440)}
current_candle = {'NQ': None, 'MNQ': None}
candle_lock = threading.Lock()

# 틱 버퍼 (심볼별, 가격+거래량) - 틱차트용
tick_buffer = {'NQ': deque(maxlen=10000), 'MNQ': deque(maxlen=10000)}
tick_lock = threading.Lock()


def get_candle_time(ts_seconds):
    return (ts_seconds // 60) * 60


def update_candle(symbol, price, volume, ts_seconds):
    global current_candle
    candle_time = get_candle_time(ts_seconds)
    
    with candle_lock:
        cc = current_candle[symbol]
        
        # 캔들 내 스파이크 필터
        if cc is not None and cc['time'] == candle_time:
            mid = (cc['high'] + cc['low']) / 2
            if abs(price - mid) > 30:
                return
        
        if cc is None or cc['time'] != candle_time:
            if cc is not None:
                candle_history[symbol].append(cc.copy())
            current_candle[symbol] = {
                'time': candle_time,
                'open': price, 'high': price, 'low': price, 'close': price,
                'volume': volume or 1,
                'tick_count': 1,
            }
        else:
            cc['high'] = max(cc['high'], price)
            cc['low'] = min(cc['low'], price)
            cc['close'] = price
            cc['volume'] = cc.get('volume', 0) + (volume or 1)
            cc['tick_count'] = cc.get('tick_count', 0) + 1
    
    # 틱 버퍼에 추가
    with tick_lock:
        tick_buffer[symbol].append({
            'time': ts_seconds,
            'price': price,
            'volume': volume or 1,
        })


def process_record(record, symbol):
    sd = symbols_data[symbol]
    price = None
    bid = None
    ask = None
    volume = 0
    
    # trade 가격
    if hasattr(record, 'price') and record.price:
        p = record.price / 1e9
        if p > 1000:
            price = round(p, 2)
    
    # bid/ask
    if hasattr(record, 'levels') and len(record.levels) > 0:
        level = record.levels[0]
        raw_bid = level.bid_px
        raw_ask = level.ask_px
        b = raw_bid / 1e9 if raw_bid and raw_bid > 0 else None
        a = raw_ask / 1e9 if raw_ask and raw_ask > 0 else None
        
        if b and b > 1000 and a and a > 1000:
            spread = a - b
            if spread < 10:
                bid = round(b, 2)
                ask = round(a, 2)
                if price is None:
                    price = round((b + a) / 2, 2)
                sd['bid'] = bid
                sd['ask'] = ask
    
    # volume (거래 건수)
    if hasattr(record, 'size') and record.size:
        volume = record.size
    
    if price and price > 1000:
        # 스파이크 필터
        prev = sd.get('price')
        if prev and abs(price - prev) > 50:
            return
        
        sd['price'] = price
        sd['volume'] = volume
        sd['timestamp'] = int(time.time() * 1000)
        sd['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
        
        update_candle(symbol, price, volume, int(time.time()))


def run_live_feed():
    """Databento Live: NQ + MNQ 동시 구독"""
    global iid_to_symbol
    api_key = os.environ.get('DATABENTO_API_KEY')
    
    if not api_key:
        feed_status['error'] = 'DATABENTO_API_KEY not set'
        return
    
    while True:
        try:
            print("🔌 Databento Live 연결 시작 (NQ + MNQ)...")
            client = db.Live(key=api_key)
            
            client.subscribe(
                dataset='GLBX.MDP3',
                schema='mbp-1',
                symbols=['NQ.c.0', 'MNQ.c.0'],
                stype_in='continuous',
            )
            
            feed_status['connected'] = True
            feed_status['error'] = None
            print("✅ NQ + MNQ 동시 수신 시작!")
            
            # 첫 번째 레코드들에서 instrument_id 매핑 수집
            mappings_found = False
            
            for record in client:
                try:
                    # instrument_id → symbol 매핑 (SymbolMappingMsg에서)
                    if hasattr(record, 'stype_in_symbol') and hasattr(record, 'instrument_id'):
                        raw = str(getattr(record, 'stype_in_symbol', ''))
                        iid = record.instrument_id
                        if 'MNQ' in raw:
                            iid_to_symbol[iid] = 'MNQ'
                            print(f"📋 매핑: instrument_id {iid} → MNQ")
                        elif 'NQ' in raw:
                            iid_to_symbol[iid] = 'NQ'
                            print(f"📋 매핑: instrument_id {iid} → NQ")
                    
                    # 데이터 레코드 처리
                    if hasattr(record, 'price') or hasattr(record, 'levels'):
                        sym = 'NQ'  # 기본
                        
                        if hasattr(record, 'instrument_id'):
                            iid = record.instrument_id
                            if iid in iid_to_symbol:
                                sym = iid_to_symbol[iid]
                            else:
                                # 매핑이 아직 없으면 raw_symbol로 판별
                                for attr in ['pretty_symbol', 'raw_symbol']:
                                    val = str(getattr(record, attr, ''))
                                    if 'MNQ' in val:
                                        sym = 'MNQ'
                                        iid_to_symbol[iid] = 'MNQ'
                                        break
                                    elif 'NQ' in val:
                                        sym = 'NQ'
                                        iid_to_symbol[iid] = 'NQ'
                                        break
                        
                        process_record(record, sym)
                    
                except Exception as e:
                    continue
                    
        except Exception as e:
            print(f"❌ Databento 연결 에러: {e}")
            feed_status['connected'] = False
            feed_status['error'] = str(e)
            print("🔄 5초 후 재연결...")
            time.sleep(5)


# ========== API ENDPOINTS ==========

@app.route('/')
def index():
    return jsonify({
        'service': 'CROWNY NQ+MNQ Price Server v2',
        'status': 'running',
        'connected': feed_status['connected'],
        'nq_candles': len(candle_history['NQ']),
        'mnq_candles': len(candle_history['MNQ']),
        'nq_ticks': len(tick_buffer['NQ']),
        'mnq_ticks': len(tick_buffer['MNQ']),
        'iid_map': {str(k): v for k, v in iid_to_symbol.items()},
    })


@app.route('/api/market/live')
def get_live_price():
    """실시간 가격 (?symbol=NQ or MNQ)"""
    symbol = request.args.get('symbol', '').upper()
    
    # symbol 미지정 시 둘 다 반환 (하위호환 + NQ 기본)
    if symbol not in ('NQ', 'MNQ'):
        # 하위호환: 기존 클라이언트는 NQ 기대
        nq = symbols_data['NQ']
        mnq = symbols_data['MNQ']
        price = nq['price'] or mnq['price']
        return jsonify({
            'symbol': 'NQ',
            'price': price,
            'bid': nq['bid'] or mnq['bid'],
            'ask': nq['ask'] or mnq['ask'],
            'volume': nq['volume'] or mnq['volume'],
            'timestamp': nq['timestamp'] or mnq['timestamp'],
            'source': 'databento-live',
            'connected': feed_status['connected'],
            'last_update': nq['last_update'] or mnq['last_update'],
            'error': feed_status['error'],
            # 개별 데이터도 포함
            'nq_price': nq['price'],
            'mnq_price': mnq['price'],
            'nq_bid': nq['bid'],
            'nq_ask': nq['ask'],
            'mnq_bid': mnq['bid'],
            'mnq_ask': mnq['ask'],
        })
    
    sd = symbols_data[symbol]
    other = symbols_data['MNQ' if symbol == 'NQ' else 'NQ']
    
    return jsonify({
        'symbol': symbol,
        'price': sd['price'] or other['price'],
        'bid': sd['bid'] or other['bid'],
        'ask': sd['ask'] or other['ask'],
        'volume': sd['volume'],
        'timestamp': sd['timestamp'] or other['timestamp'],
        'source': 'databento-live',
        'connected': feed_status['connected'],
        'last_update': sd['last_update'] or other['last_update'],
        'error': feed_status['error']
    })


@app.route('/api/market/candles')
def get_candles():
    """1분 캔들 히스토리 (볼륨 포함)"""
    symbol = request.args.get('symbol', 'NQ').upper()
    if symbol not in candle_history:
        symbol = 'NQ'
    
    limit = min(int(request.args.get('limit', 1440)), 1440)
    
    with candle_lock:
        candles = list(candle_history[symbol])
        if current_candle[symbol] is not None:
            candles.append(current_candle[symbol].copy())
    
    # fallback
    if len(candles) == 0:
        other = 'MNQ' if symbol == 'NQ' else 'NQ'
        with candle_lock:
            candles = list(candle_history[other])
            if current_candle[other] is not None:
                candles.append(current_candle[other].copy())
    
    if len(candles) > limit:
        candles = candles[-limit:]
    
    return jsonify({
        'candles': candles,
        'count': len(candles),
        'interval': '1m',
        'symbol': symbol
    })


@app.route('/api/market/ticks')
def get_ticks():
    """틱 데이터 (가격 + 거래량) - 틱차트용"""
    symbol = request.args.get('symbol', 'NQ').upper()
    if symbol not in tick_buffer:
        symbol = 'NQ'
    
    limit = min(int(request.args.get('limit', 5000)), 10000)
    
    with tick_lock:
        ticks = list(tick_buffer[symbol])
    
    if len(ticks) == 0:
        other = 'MNQ' if symbol == 'NQ' else 'NQ'
        with tick_lock:
            ticks = list(tick_buffer[other])
    
    if len(ticks) > limit:
        ticks = ticks[-limit:]
    
    return jsonify({
        'ticks': ticks,
        'count': len(ticks),
        'symbol': symbol
    })


@app.route('/api/market/health')
def health():
    return jsonify({
        'status': 'ok',
        'connected': feed_status['connected'],
        'error': feed_status['error'],
        'nq': {
            'price': symbols_data['NQ']['price'],
            'last_update': symbols_data['NQ']['last_update'],
            'candles': len(candle_history['NQ']),
            'ticks': len(tick_buffer['NQ']),
        },
        'mnq': {
            'price': symbols_data['MNQ']['price'],
            'last_update': symbols_data['MNQ']['last_update'],
            'candles': len(candle_history['MNQ']),
            'ticks': len(tick_buffer['MNQ']),
        },
        'iid_map': {str(k): v for k, v in iid_to_symbol.items()},
    })


if __name__ == '__main__':
    feed_thread = threading.Thread(target=run_live_feed, daemon=True)
    feed_thread.start()
    
    port = int(os.environ.get('PORT', 8080))
    print(f"🚀 서버 시작: 포트 {port} (NQ + MNQ 듀얼 피드)")
    app.run(host='0.0.0.0', port=port)
