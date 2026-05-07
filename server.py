import os
import json
from aiohttp import web, WSMsgType

routes = web.RouteTableDef()
clients = set()

@routes.get('/ws')
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    clients.add(ws)
    print(f"Клиент подключился. Всего: {len(clients)}")
    
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                # Пытаемся распарсить как JSON, чтобы убедиться в правильности формата
                try:
                    data = json.loads(msg.data)
                    # Рассылаем всем остальным
                    for client in clients:
                        if client != ws and not client.closed:
                            try:
                                await client.send_str(json.dumps(data))
                            except Exception:
                                pass
                except json.JSONDecodeError:
                    print("Получен невалидный JSON")
    finally:
        clients.remove(ws)
        print(f"Клиент отключился. Всего: {len(clients)}")
        
    return ws

@routes.get('/')
async def index_handler(request):
    return web.Response(text="Notion-style Chat & WebRTC Signaling Server is running.")

app = web.Application()
app.add_routes(routes)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    web.run_app(app, host='0.0.0.0', port=port)