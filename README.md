# Installation

```console
poetry install
```

# Run server

```console
poetry run hypercorn -k uvloop src.app:app
```

You can of course choose another worker class like `asyncio`

# Run many simultaneous requests

```console
xargs -I % -P 0 curl http://localhost:8000\?id\=1 < <(printf '%s\n' {1..10})
```
