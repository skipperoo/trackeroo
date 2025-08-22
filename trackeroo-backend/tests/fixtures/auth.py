import time
import base64
from jose import jwt

def gen_token(sub, secret):
    now = int(time.time())
    data = {
        "exp": now + 120,
        "iat": now,
        "sub": sub,
    }
    token = jwt.encode(
        data, base64.standard_b64decode(secret), algorithm="HS256"
    )
    return token
