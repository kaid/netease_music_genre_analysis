import time
from os import path
from typing import Any
from urllib.parse import urlencode
from aiohttp import ClientSession, CookieJar

# API courtesy of https://gitlab.com/Binaryify/NeteaseCloudMusicApi

COOKIE_FILE = '../data/cookie.pickle'
COOKIE_JAR = CookieJar()

def save_cookies():
    COOKIE_JAR.save(file_path=COOKIE_FILE)

if (not path.exists(COOKIE_FILE)):
    save_cookies()

COOKIE_JAR.load(file_path=COOKIE_FILE)

def get_url(path: str, params: Mapping[str, Any] = None) -> str:
    return f"http://localhost:3000{path}{'?' + urlencode(params) if params else ''}"

async def post_from(url: str, params: Mapping[str, Any] = {}, data: Mapping[str, Any] = {}) -> Any:
    async with ClientSession(cookie_jar=COOKIE_JAR) as session:
        async with session.post(url, data=data) as response:
            return await response.json()

async def get_qr_key() -> Any:
    return await post_from(get_url('/login/qr/key'), data={ 'timestamp': int(time.time() * 1000) })

async def get_qr_image(key: str) -> Any:
    return await post_from(get_url('/login/qr/create'), data={ 'key': key, 'qrimg': 'true' })

async def check_qr_login_status(key: str) -> Any:
    return await post_from(get_url('/login/qr/check'), data={ 'key': key })

async def get_qr_image_url() -> (str, str):
    qr_key_res = await get_qr_key()

    if not qr_key_res['code'] == 200:
        raise Exception(qr_key_res['msg'] or '获取二维码Key失败')
    qr_key = qr_key_res['data']['unikey']

    qr_image_res = await get_qr_image(qr_key)

    if not qr_image_res['code'] == 200:
        raise Exception(qr_image_res['msg'] or '获取二维码图片失败')

    return qr_image_res['data']['qrimg'], qr_key

async def is_qr_login_success(key: str) -> (str, int, str):
    res = await check_qr_login_status(key)

    code = res['code']
    cookie = res['cookie']

    match code:
        case 801 | 802:
            return ('pending', code, cookie)
        case 803:
            return ('success', code, cookie)
        case _:
            return ('failed', code, cookie)

async def get_playlists(cat: str, offset: int = 0, limit: int = 16) -> Any:
    return await post_from(get_url('/top/playlist'), data={ 'cat': cat, 'offset': offset, 'limit': limit, 'total': 'true', 'order': 'hot' })

async def get_playlist_tracks(playlist_id: int, offset: int = 0, limit: int = 100) -> dict[str, Any]:
    return await post_from(get_url('/playlist/track/all'), data={ 'id': playlist_id, 'offset': offset, 'limit': limit })

async def get_artist_detail(artist_id: int) -> dict[str, Any]:
    return await post_from(get_url('/ugc/artist/get'), data={ 'id': artist_id })

ALBUM_SALES = ''

ARTIST_RECORDS = ''