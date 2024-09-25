import pickle
from os import path
import polars as pl
from typing import TypeVar, Union, Callable, Awaitable

import utils.api as api
from utils.cache import Cache

LANGUAGE_CATEGORIES = ['华语']
GENRE_CATEGORIES = ['摇滚', '朋克', '民谣', '说唱', '电子']

ARTISTS_IDS = {
    '崔健': 1,
    '张楚': 1,
    '何勇': 1,
    '窦唯': 1,
    '黑豹乐队': 1,
    '唐朝乐队': 1,
    '郑钧': 1,
    '眼镜蛇乐队': 1,

    '老狼': 1,
    '朴树': 1,
    '水木年华': 1,
    '野孩子乐队': 1,
    '万能青年旅店': 1,
    '新裤子乐队': 1,
    '脑浊乐队': 1,
    '腰乐队': 1,
    '左小祖咒': 1,
    '二手玫瑰': 1,
    '惘闻乐队': 1,
    '谢天笑': 1,
    'AK (AK47)': 1,
    '果味VC': 1,
    '万晓利': 1,
    '低苦艾乐队': 1,
    '周云蓬': 1,
    '后海大鲨鱼': 1,
    '左右乐队': 1,
    '五条人': 1,

    '不速之客乐队': 1,
    '吹万乐队': 1,
    'Chinese Football': 1,
    'Nova Heart': 1,
    '大波浪乐队': 1,
    '海朋森乐队': 1,
    '程璧': 1,
    '达闻西乐队': 1,
    '马頔': 1,
    '张尕怂': 1,
    '陈粒': 1,
    '谢春花': 1,
    '马思唯': 1,
    '超级斩乐队': 1,
    '福禄寿乐队': 1,
}

__all__ = ['prepare_data']

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

            if not more or total <= offset:
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

async def prepare_data():
    api.save_cookies()
    ALL_PLAYLISTS = await get_or_load_from_file('../data/all_playlists.pickle', get_all_playlists)
    ALL_TRACKS = await get_or_load_from_file('../data/all_tracks.pickle', get_all_playlist_tracks)
    ALL_PLAYLIST_ARTISTS = await get_or_load_from_file('../data/playlist_artists.pickle', get_all_playlist_artists)
    api.save_cookies()

    PLAYLISTS_DF = pl.DataFrame(ALL_PLAYLISTS)
    PLAYLISTS_DF.write_parquet('../data/all_playlists.pickle')

    ARTISTS_DF = pl.DataFrame(ALL_PLAYLIST_ARTISTS)
    ARTISTS_DF.write_parquet('../data/artists_cache.pickle')

    TRACKS_DF = pl.DataFrame(ALL_TRACKS)
    TRACKS_DF.write_parquet('../data/all_playlists.pickle')
    # persist_caches()
