import pickle
from os import path
import polars as pl
from typing import TypeVar, Union, Callable, Awaitable

import utils.api as api
from utils.cache import Cache

LANGUAGE_CATEGORIES = ['华语']
GENRE_CATEGORIES = ['摇滚', '朋克', '民谣', '说唱', '电子']

ARTISTS_NAME_ID_MAP = {
    '崔健': 2111,
    '张楚': 6455,
    '何勇': 3055,
    '窦唯': 2515,
    '黑豹乐队': 11759,
    '唐朝乐队': 12972,
    '郑钧': 6458,
    '眼镜蛇乐队': 13436,

    '老狼': 3682,
    '朴树': 4721,
    '水木年华': 12712,
    '野孩子乐队': 13416,
    '万能青年旅店': 13223,
    '新裤子乐队': 13282,
    '脑浊乐队': 12442,
    '腰乐队': 13421,
    '左小祖咒': 6467,
    '二手玫瑰': 11514,
    '惘闻乐队': 13188,
    '谢天笑': 5767,
    'AK (AK47)': 10996,
    '果味VC': 11680,
    '万晓利': 5345,
    '低苦艾乐队': 11365,
    '周云蓬': 6461,
    '后海大鲨鱼': 11760,
    '左右乐队': 13594,
    '五条人': 938017,

    '不速之客乐队': 12247,
    '吹万乐队': 11353,
    'Chinese Football': 1081839,
    'Nova Heart': 12494,
    '大波浪乐队': 918003,
    '海朋森乐队': 1051015,
    '程璧': 973004,
    '达闻西乐队': 12037252,
    '马頔': 4592,
    '张尕怂': 985402,
    '陈粒': 1007170,
    '谢春花': 1039895,
    '马思唯': 1132392,
    '超级斩乐队': 13523466,
    '福禄寿乐队': 29393033,
}

__all__ = ['prepare_playlist_data']

type RawTable = dict[str, list[Union[str, int]]]

async def get_all_playlists() -> RawTable:
    limit = 100
    result: RawTable = {
        'categories': [],
        'playlist_id': [],
        'playlist_name': [],

        'play_count': [],
        'share_count': [],
        'track_count': [],
        'comment_count': [],
        'subscribed_count': [],
    }

    for cat in GENRE_CATEGORIES:
        offset = 0
        more: bool | None = None
        total: int | None = None
        while True:
            print(f'Getting {cat} playlists from {offset} to {offset + limit}, total: {total} | more: {more}')
            lists = await api.get_playlists(cat, offset, limit)

            offset += limit
            more = lists['more']

            if not total and lists['total'] > 0:
                total = lists['total']

            if not more or (total is not None and total <= offset):
                break

            for playlist in lists['playlists']:
                result['playlist_name'].append(playlist['name'])
                result['playlist_id'].append(playlist['id'])
                result['comment_count'].append(playlist['commentCount'])
                result['share_count'].append(playlist['shareCount'])
                result['subscribed_count'].append(playlist['subscribedCount'])
                result['play_count'].append(playlist['playCount'])
                result['track_count'].append(playlist['trackCount'])
                result['categories'].append(cat)

    return result

async def get_all_playlist_tracks() -> RawTable:
    limit = 500

    playlist_tracks: RawTable = { 'playlist_id': [], 'track_id': [], 'track_name': [], 'artist_id': [] }

    for index, playlist_id in enumerate(ALL_PLAYLISTS['playlist_id']):
        offset = 0
        total = int(ALL_PLAYLISTS['track_count'][index])

        while True:
            if 'TRACKS_CACHE' in globals() and playlist_id in TRACKS_CACHE:
                tracks = TRACKS_CACHE[playlist_id]
            else:
                print(f'Getting tracks from playlist {playlist_id} from {offset} to {offset + limit}, total: {total}')
                tracks = await api.get_playlist_tracks(int(playlist_id), offset, limit)
                TRACKS_CACHE[playlist_id] = tracks

            if not isinstance(tracks, dict) or 'songs' not in tracks:
                break

            for track in tracks['songs']:
                track_id = track['id']
                for artist in track['ar'] if isinstance(track['ar'], list) else []:
                    artist_id = artist['id']

                    if artist_id == 0 or not artist_id:
                        continue

                    playlist_tracks['playlist_id'].append(playlist_id)
                    playlist_tracks['track_id'].append(track_id)
                    playlist_tracks['track_name'].append(track['name'])
                    playlist_tracks['artist_id'].append(artist_id)

            offset += limit

            if offset >= total:
                break

    return playlist_tracks

async def get_all_playlist_artists() -> RawTable:
    playlist_track_artists: RawTable = {
        'track_id': [],
        'playlist_id': [],

        'artist_id': [],
        'artist_name': [],
        'artist_area': [],
        'artist_type': [],
        'artist_alias': [],
        'artist_production': [],
        'artist_description': [],
    }

    for index, artist_id in enumerate(ALL_TRACKS['artist_id']):
        track_id = ALL_TRACKS['track_id'][index]
        playlist_id = ALL_TRACKS['playlist_id'][index]

        if artist_id == 0 or not artist_id:
            continue

        if artist_id not in ARTISTS_CACHE:
            print(f'Getting artist - {artist_id} detail in playlist {index}')
            artist_detail = await api.get_artist_detail(int(artist_id))

            if 'data' not in artist_detail:
                ARTISTS_CACHE[artist_id] = None
                continue

            ARTISTS_CACHE[artist_id] = artist_detail['data']

        artist_detail = ARTISTS_CACHE[artist_id]

        if not artist_detail:
            continue

        playlist_track_artists['playlist_id'].append(playlist_id)
        playlist_track_artists['track_id'].append(track_id)
        playlist_track_artists['artist_id'].append(artist_id)
        playlist_track_artists['artist_name'].append(artist_detail['artistName'])
        playlist_track_artists['artist_area'].append(artist_detail['area'])
        playlist_track_artists['artist_type'].append(artist_detail['type'])
        playlist_track_artists['artist_description'].append(artist_detail['desc'])
        playlist_track_artists['artist_production'].append(artist_detail['production'])
        playlist_track_artists['artist_alias'].append(artist_detail['alias'])

    return playlist_track_artists

T = TypeVar('T')

async def get_or_load_from_file(file_path: str, loader: Callable[[], Awaitable[T]]) -> T:
    if not path.exists(file_path):
        result = await loader()
        with open(file_path, 'wb') as f:
            pickle.dump(result, f)
    else:
        result = pickle.load(open(file_path, 'rb'))
    return result

ALL_PLAYLISTS: RawTable = {}
ALL_TRACKS: RawTable = {}
ALL_PLAYLIST_ARTISTS: RawTable = {}

ARTISTS_CACHE = Cache('../data/artists_cache.pickle')
TRACKS_CACHE = Cache('../data/tracks_cache.pickle')

def persist_caches():
    ARTISTS_CACHE.save()
    TRACKS_CACHE.save()

async def prepare_playlist_data():
    global ALL_PLAYLISTS, ALL_TRACKS, ALL_PLAYLIST_ARTISTS

    api.save_cookies()
    ALL_PLAYLISTS = await get_or_load_from_file('../data/all_playlists.pickle', get_all_playlists)
    ALL_TRACKS = await get_or_load_from_file('../data/all_tracks.pickle', get_all_playlist_tracks)
    ALL_PLAYLIST_ARTISTS = await get_or_load_from_file('../data/playlist_artists.pickle', get_all_playlist_artists)
    api.save_cookies()

    pl.DataFrame(ALL_PLAYLISTS).write_parquet('../data/all_playlists.parquet')
    pl.DataFrame(ALL_PLAYLIST_ARTISTS).write_parquet('../data/playlist_artists.parquet')
    pl.DataFrame(ALL_TRACKS).write_parquet('../data/all_tracks.parquet')
    # persist_caches()

async def get_specific_artist_misc() -> RawTable:
    artist_misc: RawTable = {
        'artist_id': [],
        'artist_name': [],

        'mv_count': [],
        'track_count': [],
        'album_count': [],
    }

    for artist_id in list(ARTISTS_NAME_ID_MAP.values()):
        print(f'Getting artist {artist_id} misc')
        artist_detail = await api.get_artist_misc(artist_id)

        if 'artist' not in artist_detail:
            continue

        artist_detail = artist_detail['artist']

        artist_misc['artist_id'].append(artist_id)
        artist_misc['artist_name'].append(artist_detail['name'])
        artist_misc['mv_count'].append(artist_detail['mvSize'])
        artist_misc['track_count'].append(artist_detail['musicSize'])
        artist_misc['album_count'].append(artist_detail['albumSize'])

    return artist_misc

async def get_specific_artist_albums() -> RawTable:
    artist_albums: RawTable = {
        'artist_id': [],
        'album_id': [],
        'album_name': [],
    }

    for index, artist_id in enumerate(SPECIFIC_ARTIST_MISC['artist_id']):
        offset = 0
        limit = 100
        total = SPECIFIC_ARTIST_MISC['album_count'][index]

        while True:
            print(f'Getting artist {artist_id} albums from {offset} to {offset + limit}, total: {total}')
            albums = await api.get_artist_albums(int(artist_id), limit, offset)

            if 'hotAlbums' not in albums:
                break

            offset += limit

            for album in albums['hotAlbums']:
                artist_albums['artist_id'].append(artist_id)
                artist_albums['album_id'].append(album['id'])
                artist_albums['album_name'].append(album['name'])

            if offset >= int(total):
                break


    return artist_albums
    
async def get_specific_artist_sales() -> RawTable:
    artist_sales: RawTable = {
        'album_id': [],
        'album_sales': [],
    }

    album_ids = [int(album_id) for album_id in SPECIFIC_ARTIST_ALBUMS['album_id']]

    print(f'Getting artist {album_ids} sales')
    sales_data = await api.get_album_sales(album_ids)

    if 'data' not in sales_data:
        return artist_sales


    for (album_id, album_sales) in sales_data['data'].items():
        artist_sales['album_id'].append(int(album_id))
        artist_sales['album_sales'].append(album_sales)

    return artist_sales

SPECIFIC_ARTIST_MISC: RawTable = {}
SPECIFIC_ARTIST_ALBUMS: RawTable = {}
SPECIFIC_ARTIST_SALES: RawTable = {}

async def prepare_specific_artist_data():
    global SPECIFIC_ARTIST_MISC, SPECIFIC_ARTIST_ALBUMS, SPECIFIC_ARTIST_SALES

    SPECIFIC_ARTIST_MISC = await get_or_load_from_file('../data/specific_artist_misc.pickle', get_specific_artist_misc)
    SPECIFIC_ARTIST_ALBUMS = await get_or_load_from_file('../data/specific_artist_albums.pickle', get_specific_artist_albums)
    SPECIFIC_ARTIST_SALES = await get_or_load_from_file('../data/specific_artist_sales.pickle', get_specific_artist_sales)

    pl.DataFrame(SPECIFIC_ARTIST_MISC).write_parquet('../data/specific_artist_misc.parquet')
    pl.DataFrame(SPECIFIC_ARTIST_ALBUMS).write_parquet('../data/specific_artist_albums.parquet')
    pl.DataFrame(SPECIFIC_ARTIST_SALES).write_parquet('../data/specific_artist_sales.parquet')
