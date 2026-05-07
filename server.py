import os
import json
import logging
from aiohttp import web, WSMsgType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

routes = web.RouteTableDef()
clients = set()

@routes.get('/ws')
async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=30.0)
    await ws.prepare(request)
    clients.add(ws)
    
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    
                    # ПИНГ-ПОНГ логика для отображения пинга у пользователя
                    if data.get('type') == 'ping':
                        await ws.send_json({'type': 'pong'})
                        continue
                    
                    # Рассылка всем остальным
                    for client in clients:
                        if client != ws and not client.closed:
                            try:
                                await client.send_json(data)
                            except:
                                pass
                except json.JSONDecodeError:
                    pass
    finally:
        clients.discard(ws)
    return ws

@routes.get('/')
async def index_handler(request):
    return web.Response(text="Server is running")

app = web.Application()
app.add_routes(routes)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    web.run_app(app, host='0.0.0.0', port=port)
