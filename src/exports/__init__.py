import os.path
import json
from datetime import timedelta, datetime
from typing import Any, Callable, Optional, cast
import geopandas as gpd
import pandas as pd
from shapely import wkb
from shapely.affinity import scale as scale_geometry
from shapely.errors import GEOSException
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform as transform_geometry
from shapely.ops import unary_union
from sqlalchemy import text

class Export():
    '''
    Export the SF2 to a text. Default to BSF style CSV
    '''
    def __init__(self, config, db):
        with open(config) as f:
            self._config = json.load(f)
        atts = ['name','start_date','end_date','export_path','sources']
        for att in atts:
            try:
                setattr(self, att, self._config[att])
            except KeyError as e:
                raise ValueError('Missing %s in config file' %att)
        self.name: str = self._config['name']
        self.start_date: pd.Timestamp = pd.Timestamp(self._config['start_date'])
        self.end_date: pd.Timestamp = pd.Timestamp(self._config['end_date'])
        self.export_path = self._config['export_path']
        self.sources = self._config['sources']
        self.stream_id = self._get_stream_id(db)
        self._get_event_data(db)
        # Add in for retrieving HMS
        self.sources = tuple([x.lower().strip() for x in self.sources])
        self.source_ids = self._get_source_ids(db)
 
    def _get_source_ids(self, db):
        '''
        Get the source IDs of the underlying sources for the stream
        '''
        # Workaround for reconciliation streams with 1 source
        if len(self.sources) == 1:
            self.sourcekey = "('%s')" %self.sources[0]
        else:
            self.sourcekey = self.sources
        q = 'SELECT id, name FROM source WHERE name IN %(sources)s'
        q = text(q % {'sources': self.sourcekey})
        with db.engine.connect() as conn:
            df = pd.read_sql(q, con=conn)
        if len(df) < len(self.sources):
            print(df)
            raise ValueError('Missing sources. Check source names in config')
        elif len(df) > len(self.sources):
            raise ValueError('Duplicate source names in source table')
        if len(df) == 1:
            return "('%s')" %df['id'].values[0]
        else:
            return tuple(df['id'])

    def _get_stream_id(self, db):
        '''
        Get the reconciliation stream ID either from the reconciliation stream table 
        '''
        name_slug = self.name.strip().lower().replace(' ','-')
        with db.engine.connect() as conn:
            q = text("SELECT id FROM reconciliation_stream WHERE name_slug= '%s'" %name_slug)
            df = pd.read_sql(q, con=conn)
            if len(df) == 1:
                stream_id = int(str(df['id'].values[0]))
            else:
                raise ValueError('Somehow there are multiple streams with the same name')
        return stream_id

    def _get_event_data(self, db):
        '''
        Push the raw data to the postgres DB
        '''
        q = """SELECT id, display_name, start_date, end_date, total_area, fire_type FROM \
          event WHERE reconciliationstream_id = %(streamid)s AND \
          start_date <= '%(enddate)s' AND end_date >= '%(startdate)s'"""
        q = text(q % {'streamid': self.stream_id, 'startdate': self.start_date, 
          'enddate': self.end_date})
        self.events = pd.read_sql(q, con=db.engine.connect())
        self.events['start_date'] = pd.to_datetime(self.events['start_date'])
        self.events['end_date'] = pd.to_datetime(self.events['end_date'])
        self.events['e_txt_id'] = 'SF11E' + self.events['id'].astype(int).astype(str).str.zfill(8)
        self.events.rename(columns={'id': 'event_id', 
          'display_name': 'event_name', 'fire_type': 'type'}, inplace=True)
        self.events['stream_name'] = self.name
        self.events['event_name'] = self.events['event_name'].str.title()

    def _get_event_fires(self, db):
        '''
        Get the event fire xref table
        '''
        event_ids = tuple(self.events['event_id'].drop_duplicates())
        if len(event_ids) == 1:
            event_ids = (event_ids[0], -9)
        q = 'SELECT fire_id, event_id FROM event_fires WHERE event_id IN %(eventids)s'
        df = pd.read_sql(text(q %{'eventids': event_ids}), con=db.engine.connect())
        return df

    def _get_event_days(self, db):
        '''
        Get the daily event date by location
        '''
        q = """SELECT event_day.id, daily_area as area, event_date as date_time, event_id, ST_X(location) as longitude, \
          ST_Y(location) as latitude FROM event_day JOIN event ON event.id = event_day.event_id \
          WHERE event.reconciliationstream_id = %(streamid)s AND \
          event_date >= '%(startdate)s' AND event_date <= '%(enddate)s'"""
        q = text(q %{'startdate': self.start_date, 'enddate': self.end_date, 
          'streamid': self.stream_id})
        df = pd.read_sql(q, con=db.engine.connect())
        print(df.head())
        df['date_time'] = pd.to_datetime(df['date_time'])
        df['f_txt_id'] = 'SF11C' + df['id'].astype(int).astype(str).str.zfill(8)
        return df

    def _get_daily_event_shape(self, events, db):
        '''
        Get a geoJSON of the daily event shapes
        '''
        # A record of 1 needs different SQL syntax
        if len(events) == 1:
            events_str = str(events[0])
            wq = f'WHERE id = {events_str}'
        else:
            events_str = ','.join([str(s) for s in events])
            wq = f'WHERE id IN ({events_str})'
        q = """SELECT jsonb_build_object(\
          'type', 'FeatureCollection','features', jsonb_agg(feature))\
          FROM (SELECT jsonb_build_object('type', 'Feature','id', id, 'geometry',\
          ST_AsGeoJSON(ST_Transform(outline_shape, 4326))::jsonb, 'properties', \
          to_jsonb(row) - 'id' - 'outline_shape') AS feature\
          FROM (SELECT * FROM event %(where_events)s) row) features"""
        q = text(q %{'where_events': wq})
        df = pd.read_sql(q, con=db.engine.connect())
        return df.iloc[0]['jsonb_build_object']

    @staticmethod
    def _valid_polygon(
        geometry: Optional[BaseGeometry]
    ) -> Optional[BaseGeometry]:
        if geometry is None or geometry.is_empty:
            return None
        if not geometry.is_valid:
            geometry = geometry.buffer(0)
        if geometry is None or geometry.is_empty:
            return None
        if geometry.geom_type == 'GeometryCollection':
            polygons = [part for part in geometry.geoms
                        if part.geom_type in ('Polygon', 'MultiPolygon')]
            if not polygons:
                return None
            geometry = unary_union(polygons)
        if geometry is None or geometry.is_empty:
            return None
        if geometry.geom_type not in ('Polygon', 'MultiPolygon'):
            return None
        if not geometry.is_valid:
            geometry = geometry.buffer(0)
        if geometry is None or geometry.is_empty:
            return None
        return geometry if not geometry.is_empty else None

    @staticmethod
    def _project_geometry(geometry: BaseGeometry, source_crs, target_crs):
        geo_series = cast(Any, gpd.GeoSeries)
        return cast(
            BaseGeometry,
            geo_series([cast(Any, geometry)], crs=source_crs)
            .to_crs(target_crs).iloc[0]
        )

    @classmethod
    def _overlay(
        cls,
        first: BaseGeometry,
        second: BaseGeometry,
        operation: Callable[[BaseGeometry, BaseGeometry], BaseGeometry]
    ) -> Optional[BaseGeometry]:
        try:
            return operation(first, second)
        except GEOSException:
            first = cls._valid_polygon(first.buffer(0))
            second = cls._valid_polygon(second.buffer(0))
            if first is None or second is None:
                return None
            try:
                return operation(first, second)
            except GEOSException:
                coordinate_scale = max(
                    1.0, *(abs(value) for value in first.bounds + second.bounds)
                )
                grid_size = coordinate_scale * 1e-9

                def round_coordinates(x, y, z=None):
                    try:
                        rounded_x = tuple(
                            round(value / grid_size) * grid_size for value in x
                        )
                        rounded_y = tuple(
                            round(value / grid_size) * grid_size for value in y
                        )
                    except TypeError:
                        rounded_x = round(x / grid_size) * grid_size
                        rounded_y = round(y / grid_size) * grid_size
                    if z is None:
                        return rounded_x, rounded_y
                    return rounded_x, rounded_y, z

                first = cls._valid_polygon(
                    transform_geometry(round_coordinates, first).buffer(0)
                )
                second = cls._valid_polygon(
                    transform_geometry(round_coordinates, second).buffer(0)
                )
                if first is None or second is None:
                    return None
                return operation(first, second)

    @classmethod
    def _intersection(
        cls, first: BaseGeometry, second: BaseGeometry
    ) -> Optional[BaseGeometry]:
        return cls._overlay(first, second, lambda left, right: left.intersection(right))

    @classmethod
    def _difference(
        cls, first: BaseGeometry, second: BaseGeometry
    ) -> Optional[BaseGeometry]:
        return cls._overlay(first, second, lambda left, right: left.difference(right))

    @classmethod
    def _clean_daily_shape(
        cls, geometry: Optional[BaseGeometry], available: BaseGeometry
    ) -> Optional[BaseGeometry]:
        geometry = cls._valid_polygon(geometry)
        if geometry is None:
            return None
        simplified = cls._valid_polygon(
            geometry.simplify(cls.DAILY_SHAPE_SIMPLIFY_METERS, preserve_topology=True)
        )
        if simplified is None:
            return None
        parts = list(simplified.geoms) if simplified.geom_type == 'MultiPolygon' else [simplified]
        substantial_parts = [
            part for part in parts if part.area >= cls.MIN_DAILY_PART_AREA_SQM
        ]
        if not substantial_parts:
            substantial_parts = [max(parts, key=lambda part: part.area)]
        return cls._valid_polygon(unary_union(substantial_parts))

    area_tolerance = 0.01
    MAX_DAILY_OVERLAP_RATIO = 0.10
    MAX_EVENT_EXTENSION_RATIO = 0.10

    @classmethod
    def _daily_shape_within_tolerances(
        cls,
        polygon: BaseGeometry,
        event_polygon: BaseGeometry,
        target_area: float,
        previous_burned_area: Optional[BaseGeometry] = None,
    ) -> bool:
        outside = cls._difference(polygon, event_polygon)
        if outside is None:
            return False
        allowed_extension = target_area * cls.MAX_EVENT_EXTENSION_RATIO
        if outside.area > allowed_extension + cls.area_tolerance:
            return False
        if previous_burned_area is not None:
            overlap = cls._intersection(polygon, previous_burned_area)
            if overlap is None:
                return False
            allowed_overlap = target_area * cls.MAX_DAILY_OVERLAP_RATIO
            if overlap.area > allowed_overlap + cls.area_tolerance:
                return False
        return True

    @classmethod
    def _allocate_daily_shape(
        cls,
        activity: BaseGeometry,
        available: BaseGeometry,
        target_area: float,
        previous_burned_area: Optional[BaseGeometry] = None,
    ) -> Optional[BaseGeometry]:
        '''Allocate area nearest to the original daily activity geometry.'''
        active_area = cls._clean_daily_shape(
            cls._intersection(activity, available), available
        )
        if active_area is not None and active_area.area >= target_area - cls.area_tolerance:
            factor = (target_area / active_area.area) ** 0.5
            scaled_area = cls._valid_polygon(scale_geometry(
                active_area, factor, factor, origin=active_area.centroid
            ))
            scaled_area = cls._clean_daily_shape(scaled_area, available)
            if (
                scaled_area is not None
                and scaled_area.area >= target_area - cls.area_tolerance
                and cls._daily_shape_within_tolerances(
                    scaled_area, available, target_area, previous_burned_area
                )
            ):
                return scaled_area

        def nearby_area(distance):
            return cls._clean_daily_shape(
                activity.buffer(distance), available
            )

        low, high = 0.0, 1.0
        candidate = nearby_area(high)
        if candidate is None or candidate.area < target_area - cls.area_tolerance:
            for _ in range(20):
                low, high = high, high * 2.0
                candidate = nearby_area(high)
                if (
                    candidate is not None
                    and candidate.area >= target_area - cls.area_tolerance
                    and cls._daily_shape_within_tolerances(
                        candidate, available, target_area, previous_burned_area
                    )
                ):
                    break
        if (
            candidate is not None
            and candidate.area >= target_area - cls.area_tolerance
            and cls._daily_shape_within_tolerances(
                candidate, available, target_area, previous_burned_area
            )
        ):
            for _ in range(15):
                distance = (low + high) / 2.0
                expanded = nearby_area(distance)
                if (
                    expanded is not None
                    and expanded.area >= target_area - cls.area_tolerance
                    and cls._daily_shape_within_tolerances(
                        expanded, available, target_area, previous_burned_area
                    )
                ):
                    high, candidate = distance, expanded
                    if candidate.area <= target_area + cls.area_tolerance:
                        break
                else:
                    low = distance
                if high - low <= max(1e-8, high * 1e-12):
                    break
            return candidate

        return None

    def _get_daily_fire_extent_shape(
        self, events, day, db, previous_burned_area_by_event=None,
        multi_day_event_ids=None
    ):
        '''Build daily event polygons from clumps and allocate prior-day overlap.'''
        if not events:
            return {'type': 'FeatureCollection', 'features': []}

        event_ids = ','.join(str(int(event_id)) for event_id in events)
        event_day = day.strftime('%Y-%m-%d')
        query = text(f'''WITH daily_areas AS (
          SELECT event_id,
            SUM(ROUND((daily_area / 4046.8564224)::numeric, 2)
              * 4046.8564224) AS daily_area,
            MIN(id) AS event_day_id
          FROM event_day
          WHERE event_id IN ({event_ids}) AND event_date = '{event_day}'
          GROUP BY event_id
        )
        SELECT event.id AS event_id, event.display_name AS event_name,
          event.start_date AS event_start_date, event.end_date AS event_end_date,
          event.fire_type AS fire_type, event.total_area AS event_area,
          event.outline_shape, daily_areas.event_day_id, daily_areas.daily_area,
          ST_AsBinary(clump.shape) AS clump_wkb,
          ST_X(event_day.location) AS longitude,
          ST_Y(event_day.location) AS latitude
        FROM daily_areas
        JOIN event ON event.id = daily_areas.event_id
        JOIN event_day ON event_day.event_id = daily_areas.event_id
          AND event_day.event_date = '{event_day}'
        LEFT JOIN clump ON clump.id = event_day.clump_id''')
        with db.engine.connect() as conn:
            daily_rows = cast(
                gpd.GeoDataFrame,
                gpd.read_postgis(
                    query, con=conn, geom_col='outline_shape', chunksize=None
                )
            )
        if daily_rows.empty:
            return {'type': 'FeatureCollection', 'features': []}

        features = []
        for event_id, rows in daily_rows.groupby('event_id', sort=False):
            event = rows.iloc[0]
            event_id = int(str(event_id))
            outline = self._valid_polygon(cast(BaseGeometry, event['outline_shape']))
            if outline is None:
                continue
            crs = cast(Any, daily_rows.crs)
            outline_geo = None

            track_previous_area = (
                previous_burned_area_by_event is not None
                and multi_day_event_ids is not None
                and event_id in multi_day_event_ids
            )
            cumulative_burned_area = None
            burned_area = None
            if track_previous_area:
                previous = previous_burned_area_by_event.get(event_id)
                if previous is not None:
                    previous_crs, cumulative_burned_area = previous
                    if previous_crs != crs:
                        cumulative_burned_area = self._project_geometry(
                            cumulative_burned_area, previous_crs, crs
                        )
                    cumulative_burned_area = self._valid_polygon(
                        cumulative_burned_area
                    )
                    if cumulative_burned_area is not None:
                        burned_area = self._valid_polygon(
                            self._intersection(cumulative_burned_area, outline)
                        )
            available = outline
            activity_shapes = []
            for row in rows.itertuples():
                activity = None
                if isinstance(row.clump_wkb, (bytes, bytearray, memoryview)):
                    clump = self._valid_polygon(
                        wkb.loads(bytes(row.clump_wkb))
                    )
                    if clump is not None:
                        activity = self._valid_polygon(
                            self._intersection(clump, outline)
                        )
                if activity is None:
                    point = Point(float(str(row.longitude)), float(str(row.latitude)))
                    if outline_geo is None:
                        outline_geo = self._valid_polygon(
                            self._project_geometry(outline, crs, 4326)
                        )
                    if outline_geo is None:
                        continue
                    if outline_geo.covers(point):
                        point = self._project_geometry(point, 4326, crs)
                    else:
                        point = outline.representative_point()
                    activity = self._valid_polygon(
                        self._intersection(point.buffer(1.0), outline)
                    )
                if activity is not None:
                    activity_shapes.append(activity)
            if not activity_shapes:
                continue

            activity = self._valid_polygon(unary_union(activity_shapes))
            if activity is None:
                continue
            target_area = float(str(event['daily_area']))
            if target_area <= 0:
                continue
            polygon = self._allocate_daily_shape(
                activity, available, target_area, burned_area
            )
            if polygon is None or polygon.is_empty:
                continue
            if track_previous_area:
                cumulative_burned_area = (
                    polygon if cumulative_burned_area is None else self._overlay(
                        cumulative_burned_area,
                        polygon,
                        lambda left, right: left.union(right)
                    )
                )
                cumulative_burned_area = self._valid_polygon(
                    cumulative_burned_area
                )
                if cumulative_burned_area is not None:
                    previous_burned_area_by_event[event_id] = (
                        crs, cumulative_burned_area
                    )
            geojson_geometry = self._project_geometry(
                polygon, crs, 4326
            ).__geo_interface__
            acres = target_area / self.ACRES_TO_SQM
            features.append({
                'type': 'Feature',
                'id': int(str(event_id)),
                'geometry': geojson_geometry,
                'properties': {
                    'event_id': int(str(event_id)),
                    'f_txt_id': f"SF11C{int(str(event['event_day_id'])):08d}",
                    'event_name': event['event_name'],
                    'fire_type': event['fire_type'],
                    'date': event_day,
                    'event_start_date': pd.Timestamp(
                        event['event_start_date']
                    ).strftime('%Y-%m-%d'),
                    'event_end_date': pd.Timestamp(
                        event['event_end_date']
                    ).strftime('%Y-%m-%d'),
                    'daily_area': round(acres, 2),
                    'daily_area_sqm': target_area,
                    'event_day_area_sqm': target_area,
                    'event_area': round(
                        float(str(event['event_area'])) / self.ACRES_TO_SQM, 2
                    )
                }
            })

        return {'type': 'FeatureCollection', 'features': features}

    def _get_sources(self, db):
        '''
        Get the sources used in each reconciled event as semicolon concatted list
        '''
        event_fires = self._get_event_fires(db)
        fire_ids = tuple(event_fires['fire_id'].drop_duplicates())        
        if len(fire_ids) == 1:
            fire_ids = (fire_ids[0], -9)
        q = 'SELECT fire.id as fire_id, source.name as sources FROM fire LEFT JOIN source ON \
          source.id = fire.source_id WHERE fire.id IN %(fireids)s'
        fire_sources = pd.read_sql(text(q %{'fireids': fire_ids}), con=db.engine.connect())
        event_fires = event_fires.merge(fire_sources, on='fire_id')
        event_fires.drop_duplicates(['event_id','sources'], inplace=True)
        event_fires = event_fires.groupby('event_id')['sources'].apply(lambda x: ';'.join(x))
        return event_fires.reset_index()

    ACRES_TO_SQM = 4046.8564224
    DAILY_SHAPE_SIMPLIFY_METERS = 1.0
    MIN_DAILY_PART_AREA_SQM = 400.0
    def _write_events(self, df, fn):
        '''
        Write the BSF-style events file
        '''
        df['id'] = df['e_txt_id']
        # Convert area from sqm to acres
        df['total_area'] = (df['total_area'] / self.ACRES_TO_SQM).round(2)
        cols = ['id','event_name','stream_name','start_date','end_date','total_area','sources']
        df.to_csv(os.path.join(self.export_path, fn), index=False, columns=cols)

    def _write_locations(self, df, fn):
        '''
        Write the BSF-style locations file
        '''
        df['id'] = df['f_txt_id']
        df['event_id'] = df['e_txt_id']
        # Convert area from sqm to acres
        df['area'] = (df['area'] / self.ACRES_TO_SQM).round(2)
        cols = ['id','event_id','event_name','latitude','longitude','date_time','area',
          'type','stream_name','detect_cnt','mean_frp']
        df.to_csv(os.path.join(self.export_path, fn), index=False, columns=cols)

    def _write_shapes(self, daily_shapes, fn):
        '''
        Write the GeoJSON shape files
        '''
        fn = os.path.join(self.export_path, fn)
        with open(fn, 'w') as f:
            json.dump(daily_shapes, f, indent=2) 

    def export(self, db):
        '''
        Default export method to BSF ready format
        '''
        event_days = self._get_event_days(db)
        event_sources = self._get_sources(db)
        sat = self._get_detect_data(db)
        self.events = self.events.merge(event_sources, on='event_id', how='left')
        for day in list(pd.date_range(self.start_date, self.end_date)):
            day_str = datetime.strftime(day, '%Y%m%d')
            print('Exporting %s' %day_str)
            event_fn = 'events_%s.csv' %day_str
            day_event_ids = event_days.loc[
              event_days['date_time'] == day, 'event_id'
            ].drop_duplicates()
            day_events = self.events[
              self.events['event_id'].isin(day_event_ids)
            ].drop_duplicates('event_id').copy()
            self._write_events(day_events, event_fn)
            day_locs = event_days[event_days['date_time'] == day].copy()
            day_locs = day_locs.merge(day_events, on='event_id', suffixes=['','_ev'])
            day_locs = day_locs.merge(sat, on=['event_id','date_time'], how='left')
            loc_fn = 'fire_locations_%s.csv' %day_str
            self._write_locations(day_locs, loc_fn)

    def export_json(self, db):
        '''
        Export total event polygons to geojson
        '''
        event_days = self._get_event_days(db)
        event_sources = self._get_sources(db)
        sat = self._get_detect_data(db)
        self.events = self.events.merge(event_sources, on='event_id', how='left')
        for day in list(pd.date_range(self.start_date, self.end_date)):
            day_str = datetime.strftime(day, '%Y%m%d')
            print('Exporting %s' %day_str)
            event_fn = 'events_%s.csv' %day_str
            idx = (self.events['start_date'] <= day) & (self.events['end_date'] >= day)
            day_events = self.events[idx].drop_duplicates('event_id').copy()
            if not day_events.empty:
                daily_shapes = self._get_daily_event_shape(list(day_events.event_id), db)
                shp_fn = f'fire_events_{day_str}.json'
                self._write_shapes(daily_shapes, shp_fn)

    def export_daily_fire_extents(self, db):
        '''
        Export event polygons scaled to daily area and aligned with daily activity
        '''
        event_days = self._get_event_days(db)
        multi_day_event_ids = set(
            event_days.groupby('event_id')['date_time'].nunique()
            .loc[lambda counts: counts > 1].index
        )
        previous_burned_area_by_event = {}
        for day in list(pd.date_range(self.start_date, self.end_date)):
            day_str = datetime.strftime(day, '%Y%m%d')
            day_event_ids = event_days.loc[
              event_days['date_time'] == day, 'event_id'
            ].drop_duplicates()
            print(f'Exporting {len(day_event_ids)} fire extents for {day_str}')
            if not day_event_ids.empty:
                daily_shapes = self._get_daily_fire_extent_shape(
                  list(day_event_ids), day, db, previous_burned_area_by_event,
                  multi_day_event_ids)
                shp_fn = f'fire_daily_{day_str}.json'
                self._write_shapes(daily_shapes, shp_fn)

    def _get_rawsatdata(self, db, srcids):
        '''
        Get the rawdata and clump ids from the raw data table for the selected sources
        '''
        if len(srcids) == 1:
            q = f'SELECT id as rawdata_id, clump_id, start_date FROM raw_data where source_id = {srcids[0]}'
        else:
            q = 'SELECT id as rawdata_id, clump_id, start_date FROM raw_data where source_id IN %(sourceids)s'
            q = text(q %{'sourceids': srcids})
        return pd.read_sql(q, con=db.engine.connect())

    def _get_viirs_frp(self, db, rdids):
        '''
        Get the VIIRS FRP values from the rawdata attribute table
        '''
        # Updated the raw data ID list only for VIIRS
        q = """SELECT rawdata_id FROM data_attribute \
          WHERE name = 'Method' AND attr_value = 'VIIRS' and rawdata_id IN %(rawids)s"""
        q = text(q %{'rawids': rdids})
        df = pd.read_sql(q, con=db.engine.connect())
        # Get the FRP values for VIIRS
        q = """SELECT attr_value as frp, raw_data.clump_id, rawdata_id FROM data_attribute \
          LEFT JOIN raw_data ON data_attribute.rawdata_id = raw_data.id \
          WHERE name = 'FRP' AND rawdata_id IN %(rawids)s"""
        q = text(q %{'rawids': tuple(df.rawdata_id.drop_duplicates())})
        df = pd.read_sql(q, con=db.engine.connect())
        df['frp'] = df['frp'].astype(float)
        return df[df['frp'] > 0].copy()

    def _get_detect_data(self, db):
        '''
        Export HMS detect count and mean clump VIIRS FRP per daily event ID
        '''
        # Get the source IDs for the sources in this stream that have an hms clump method
        q = """SELECT source.id as source_id FROM source WHERE source.id IN %(sourceids)s AND \
          clump_method = 'hms'"""
        q = text(q %{'sourceids': self.source_ids})
        df = pd.read_sql(q, con=db.engine.connect())
        sat_srcs = tuple(df.source_id.drop_duplicates())
        if len(sat_srcs) == 0:
            print('No satellite sources in stream. Nothing to do.')
            sat = pd.DataFrame(columns=['event_id','detect_cnt','mean_frp','date_time'])
        else:
            rawdata = self._get_rawsatdata(db, sat_srcs)
            detect_cnt = rawdata.groupby(['clump_id','start_date'], as_index=False).count()
            # Get the fire IDs for the clumps
            q = 'SELECT id as clump_id, event_id FROM clump LEFT JOIN event_fires \
              ON clump.fire_id = event_fires.fire_id WHERE clump.id IN %(clumps)s'
            q = text(q %{'clumps': tuple(detect_cnt.clump_id.drop_duplicates())})
            cf_xref = pd.read_sql(q, con=db.engine.connect())
            # Sum up the number of detects per event_day
            detect_cnt = detect_cnt.merge(cf_xref, on='clump_id', how='left')
            idx = ['event_id','start_date']
            detect_cnt = detect_cnt[idx+['rawdata_id',]].groupby(idx, as_index=False).sum()
            # Get the mean clump VIIRS FRP for the fire
            frp = self._get_viirs_frp(db, tuple(rawdata.rawdata_id.drop_duplicates()))
            frp = frp.merge(rawdata[['rawdata_id','start_date']], on='rawdata_id', how='left')
            frp = frp.merge(cf_xref, on='clump_id', how='left')
            frp = frp[idx+['frp',]].groupby(idx, as_index=False).mean()
            # Every fire ID with FRP should have a detect count but every detect count may not have FRP
            sat = detect_cnt.merge(frp, on=idx, how='left')
            sat['frp'] = sat['frp'].round(4)
            sat['start_date_time'] = pd.to_datetime(sat['start_date'])
            colmap = {'rawdata_id': 'detect_cnt', 'frp': 'mean_frp', 'start_date': 'date_time'}
            sat.rename(columns=colmap, inplace=True)
        return sat
    start_date = None # type: ignore
