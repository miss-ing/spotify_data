import sqlalchemy as db



metadata = db.MetaData()

tracks = db.Table(
    "tracks",
    metadata,
    db.Column("track_id", db.String, primary_key=True),
    db.Column("track_name", db.String),
    db.Column("album_id", db.String, db.ForeignKey("albums.album_id")),
    db.Column("primary_artist_id", db.String, db.ForeignKey("artists.artist_id")),
    db.Column("duration_ms", db.Integer),
    db.Column("explicit", db.Boolean),
    db.Column("disc_number", db.Integer),
    db.Column("track_number", db.Integer),
    db.Column("isrc_id", db.String)

)

albums = db.Table(
    "albums",
    metadata,
    db.Column("album_id", db.String, primary_key=True),
    db.Column("album_name", db.String),
    db.Column("primary_artist_id", db.String, db.ForeignKey("artists.artist_id")),
    db.Column("release_year", db.Integer),
    db.Column("upc_id", db.String),
    db.Column("label", db.String),
    db.Column("popularity", db.Integer)
)

artists = db.Table(
    "artists",
    metadata,
    db.Column("artist_id", db.String, primary_key = True),
    db.Column("artist_name", db.String),
    db.Column("followers", db.Integer),
    db.Column("popularity", db.Integer)
)

tracks_artists = db.Table(
    "tracks_artists",
    metadata,
    db.Column("track_id", db.String, db.ForeignKey("tracks.track_id")),
    db.Column("artist_id", db.String, db.ForeignKey("artists.artist_id"))
)

albums_artists = db.Table(
    "albums_artists",
    metadata,
    db.Column("album_id", db.String, db.ForeignKey("albums.album_id")),
    db.Column("artist_id", db.String, db.ForeignKey("artists.artist_id"))
)

artists_genres = db.Table(
    "artists_genres",
    metadata,
    db.Column("artist_id", db.String, db.ForeignKey("artists.artist_id")),
    db.Column("genre", db.String)
)


streams = db.Table(
    "streams",
    metadata,
    db.Column("stream_id",db.String),
    db.Column("date", db.Date),
    db.Column("time", db.Time),
    db.Column("day_of_week",db.String),
    db.Column("track_id",db.String, db.ForeignKey("tracks.track_id")),
    db.Column("ms_played", db.Integer),
    db.Column("reason_start",db.String),
    db.Column("reason_end",db.String),
    db.Column("shuffle",db.Boolean),
    db.Column("skipped",db.Boolean),
    #db.Column("offline_timestamp", db.String),
    db.Column("incognito_mode",db.Boolean),
    db.Column("ip_address",db.String),
    db.Column("country", db.String),
    db.Column("platform",db.String)
)

