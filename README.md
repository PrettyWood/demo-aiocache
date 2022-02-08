# Installation

```console
poetry install
```

# Run server and executor

### Server

You need to run on the one side the ASGI web server that will handle
HTTP requests and put queries in Redis as message broker

```console
poetry run hypercorn -k uvloop src.app:app
```

(You can of course choose another worker class like `asyncio`)

### Executor

You need to run on the other side the executor that will run asynchronously
and ensure computing is run only when needed. All the recent computings will indeed be stored in cache.

```console
poetry run python executor.py
```

# Run many simultaneous requests

```console
xargs -I % -P 0 curl http://localhost:8000\?id\=1 < <(printf '%s\n' {1..10})
```
