import os
from aiohttp import web

routes = web.RouteTableDef()
# Множество для хранения всех активных WebSocket-соединений
clients = set()

@routes.get('/ws')
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    clients.add(ws)
    print("Новый клиент подключился.")
    
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                # Рассылаем сообщение всем, кроме отправителя
                for client in clients:
                    if client != ws:
                        await client.send_str(msg.data)
            elif msg.type == web.WSMsgType.ERROR:
                print(f'Соединение закрыто с ошибкой {ws.exception()}')
    finally:
        clients.remove(ws)
        print("Клиент отключился.")
        
    return ws

# Простая "заглушка" для корневого пути (чтобы Render показывал, что сервис жив)
@routes.get('/')
async def index_handler(request):
    return web.Response(text="Chat WebSocket Server is running. Connect to /ws")

app = web.Application()
app.add_routes(routes)

if __name__ == '__main__':
    # Render передает порт через переменную окружения PORT
    port = int(os.environ.get('PORT', 8000))
    web.run_app(app, host='0.0.0.0', port=port)