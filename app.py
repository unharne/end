import uuid
from aiohttp import web

routes = web.RouteTableDef()

USERS = {}
MESSAGES = []
THREADS = {}

@routes.post('/join')
async def join(request):
    data = await request.json()
    user_id = str(uuid.uuid4())

    USERS[user_id] = {
        "nickname": data.get("nickname", "Anon"),
        "avatar": data.get("avatar", "🙂"),
        "color": data.get("color", "#333")
    }

    return web.json_response({"user_id": user_id})

@routes.post('/message')
async def message(request):
    data = await request.json()

    msg = {
        "id": str(uuid.uuid4()),
        "user_id": data["user_id"],
        "text": data["text"]
    }

    MESSAGES.append(msg)
    return web.json_response({"status": "ok"})

@routes.get('/messages')
async def get_messages(request):
    return web.json_response(MESSAGES[-100:])

@routes.post('/thread')
async def create_thread(request):
    data = await request.json()

    thread_id = str(uuid.uuid4())
    THREADS[thread_id] = {
        "title": data["title"],
        "messages": []
    }

    return web.json_response({"thread_id": thread_id})

@routes.post('/thread/message')
async def thread_message(request):
    data = await request.json()

    THREADS[data["thread_id"]]["messages"].append({
        "text": data["text"],
        "user_id": data["user_id"]
    })

    return web.json_response({"status": "ok"})

@routes.get('/threads')
async def get_threads(request):
    return web.json_response(THREADS)

app = web.Application()
app.add_routes(routes)

if __name__ == '__main__':
    web.run_app(app, port=8080)