import os
import json
from aiohttp import web, WSMsgType

routes = web.RouteTableDef()
# Множество для хранения всех активных WebSocket-соединений
clients = set()

@routes.get('/ws')
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    clients.add(ws)
    print(f"Подключился новый клиент. Всего: {len(clients)}")
    
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                # Мы ожидаем JSON от клиента. Просто пересылаем его всем ДРУГИМ.
                # Клиент сам отрисует свое сообщение сразу после отправки.
                message_data = msg.data
                
                # Создаем задачу на отправку, чтобы не блокировать цикл
                for client in clients:
                    if client != ws and not client.closed:
                        # Используем try/except внутри цикла, чтобы сбой одного
                        # клиента не прервал рассылку остальным
                        try:
                            await client.send_str(message_data)
                        except Exception:
                            # Если отправить не удалось, клиент вероятно отпал
                            pass
                            
            elif msg.type == WSMsgType.ERROR:
                print(f'Соединение закрыто с ошибкой: {ws.exception()}')
    finally:
        clients.remove(ws)
        print(f"Клиент отключился. Всего: {len(clients)}")
        
    return ws

@routes.get('/')
async def index_handler(request):
    return web.Response(text="Notion-style Chat Backend running.")

app = web.Application()
app.add_routes(routes)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    # '0.0.0.0' обязателен для Render
    web.run_app(app, host='0.0.0.0', port=port)