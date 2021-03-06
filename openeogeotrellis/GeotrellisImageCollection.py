import json
import logging
import math
import os
import pathlib
import subprocess
import tempfile
import uuid
from datetime import datetime, date
from typing import Dict, List, Union, Tuple, Iterable, Callable

import geopyspark as gps
import numpy as np
import pandas as pd
import pyproj
import pytz
from geopyspark import TiledRasterLayer, TMS, Pyramid, Tile, SpaceTimeKey, SpatialKey, Metadata
from geopyspark.geotrellis import Extent
from geopyspark.geotrellis.constants import CellType

from openeo_driver.backend import ServiceMetadata

try:
    from openeo_udf.api.base import UdfData, RasterCollectionTile, SpatialExtent

except ImportError as e:
    from openeo_udf.api.udf_data import UdfData
    from openeo_udf.api.raster_collection_tile import  RasterCollectionTile
    from openeo_udf.api.spatial_extent import SpatialExtent

from pandas import Series
from shapely.geometry import Point, Polygon, MultiPolygon, GeometryCollection
import xarray as xr

from openeo.imagecollection import ImageCollection, CollectionMetadata
from openeo_driver.save_result import AggregatePolygonResult
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.service_registry import SecondaryService, AbstractServiceRegistry

_log = logging.getLogger(__name__)


class GeotrellisTimeSeriesImageCollection(ImageCollection):

    def __init__(self, pyramid: Pyramid, service_registry: AbstractServiceRegistry, metadata: CollectionMetadata = None):
        super().__init__(metadata=metadata)
        self.pyramid = pyramid
        self.tms = None
        self._service_registry = service_registry
        # TODO get rid of this _band_index stuff. See https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
        self._band_index = 0

    def apply_to_levels(self, func):
        """
        Applies a function to each level of the pyramid. The argument provided to the function is of type TiledRasterLayer

        :param func:
        :return:
        """
        pyramid = Pyramid({k:func( l ) for k,l in self.pyramid.levels.items()})
        return GeotrellisTimeSeriesImageCollection(pyramid, self._service_registry, metadata=self.metadata)._with_band_index(self._band_index)

    def _apply_to_levels_geotrellis_rdd(self, func):
        """
        Applies a function to each level of the pyramid. The argument provided to the function is the Geotrellis ContextRDD.

        :param func:
        :return:
        """

        def create_tilelayer(contextrdd, layer_type, zoom_level):
            jvm = gps.get_spark_context()._gateway.jvm
            spatial_tiled_raster_layer = jvm.geopyspark.geotrellis.SpatialTiledRasterLayer
            temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer

            if layer_type == gps.LayerType.SPATIAL:
                srdd = spatial_tiled_raster_layer.apply(jvm.scala.Option.apply(zoom_level),contextrdd)
            else:
                srdd = temporal_tiled_raster_layer.apply(jvm.scala.Option.apply(zoom_level),contextrdd)

            return gps.TiledRasterLayer(layer_type, srdd)

        pyramid = Pyramid({k:create_tilelayer(func( l.srdd.rdd(),k ),l.layer_type,k) for k,l in self.pyramid.levels.items()})
        return GeotrellisTimeSeriesImageCollection(pyramid, self._service_registry, metadata=self.metadata)._with_band_index(self._band_index)

    def _with_band_index(self, band_index):
        # TODO get rid of this _band_index stuff. See https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
        self._band_index = band_index
        return self

    def band_filter(self, bands) -> 'ImageCollection':
        # TODO get rid of this _band_index stuff. See https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
        # TODO also rename to filter_bands for better consistency
        if isinstance(bands, int):
            self._band_index = bands
        elif isinstance(bands, list) and len(bands) == 1:
            self._band_index = bands[0]

        return self if self._data_source_type().lower() == 'file' else self.apply_to_levels(lambda rdd: rdd.bands(bands))

    def _data_source_type(self):
        return self.metadata.get("_vito", "data_source", "type", default="Accumulo")

    def date_range_filter(self, start_date: Union[str, datetime, date],end_date: Union[str, datetime, date]) -> 'ImageCollection':
        return self.apply_to_levels(lambda rdd: rdd.filter_by_times([pd.to_datetime(start_date),pd.to_datetime(end_date)]))

    def filter_bbox(self, west, east, north, south, crs=None, base=None, height=None) -> 'ImageCollection':
        # Note: the bbox is already extracted in `apply_process` and applied in `GeoPySparkLayerCatalog.load_collection` through the viewingParameters
        return self

    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        #TODO apply .bands(bands)
        #TODO deprecated
        return self.apply_to_levels(lambda rdd: rdd.convert_data_type(CellType.FLOAT64).map_cells(bandfunction))

    def apply(self, process:str, arguments = {}) -> 'ImageCollection':
        pysc = gps.get_spark_context()
        return self._apply_to_levels_geotrellis_rdd(lambda rdd,k: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().applyProcess( rdd,process))

    def reduce(self, reducer: str, dimension: str) -> 'ImageCollection':
        from .numpy_aggregators import var_composite, std_composite, min_composite, max_composite, sum_composite

        reducer = self._normalize_reducer(dimension, reducer)

        if reducer == 'Variance':
            return self._aggregate_over_time_numpy(var_composite)
        elif reducer == 'StandardDeviation':
            return self._aggregate_over_time_numpy(std_composite)
        elif reducer == 'Min':
            return self._aggregate_over_time_numpy(min_composite)
        elif reducer == 'Max':
            return self._aggregate_over_time_numpy(max_composite)
        elif reducer == 'Sum':
            return self._aggregate_over_time_numpy(sum_composite)
        else:
            return self.apply_to_levels(lambda layer: layer.to_spatial_layer().aggregate_by_cell(reducer))

    def reduce_bands(self,pgVisitor) -> 'ImageCollection':
        """
        TODO Define in super class? API is not yet ready for client side...
        :param pgVisitor:
        :return:
        """
        pysc = gps.get_spark_context()
        float_datacube = self.apply_to_levels(lambda layer : layer.convert_data_type("float32"))
        result = float_datacube._apply_to_levels_geotrellis_rdd(
            lambda rdd, level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().mapBands(rdd, pgVisitor.builder))
        return result

    def _normalize_reducer(self, dimension, reducer):
        if dimension != 'temporal':
            raise AttributeError('Reduce process only works on temporal dimension. Requested dimension: ' + str(dimension))
        if (reducer.upper() in ["MIN", "MAX", "SUM", "MEAN", "VARIANCE"] or reducer.upper() == "SD"):
            if reducer.upper() == "SD":
                reducer = "StandardDeviation"
            else:
                reducer = reducer.lower().capitalize()
        else:
            raise NotImplementedError("The reducer is not supported by the backend: " + reducer)
        return reducer

    @classmethod
    def _mapTransform(cls, layoutDefinition, spatialKey):
        ex = layoutDefinition.extent
        x_range = ex.xmax - ex.xmin
        xinc = x_range / layoutDefinition.tileLayout.layoutCols
        yrange = ex.ymax - ex.ymin
        yinc = yrange / layoutDefinition.tileLayout.layoutRows
        return SpatialExtent(
            top=ex.ymax - yinc * spatialKey.row,
            bottom=ex.ymax - yinc * (spatialKey.row + 1),
            right=ex.xmin + xinc * (spatialKey.col + 1),
            left=ex.xmin + xinc * spatialKey.col,
            height=layoutDefinition.tileLayout.tileCols,
            width=layoutDefinition.tileLayout.tileRows
        )

    @classmethod
    def _tile_to_datacube(cls, bands_numpy: np.ndarray, extent: SpatialExtent,
                          bands_metadata: List[CollectionMetadata.Band], start_times=None):
        from openeo_udf.api.datacube import DataCube
        coords = {}
        dims = ('bands','x', 'y')
        if len(bands_numpy.shape) == 4:
            #we have a temporal dimension
            coords = {'t':start_times}
            dims = ('t' ,'bands','x', 'y')
        if bands_metadata is not None and len(bands_metadata) > 0:
            band_names = [ m.name for m in bands_metadata]
            coords['bands']=band_names
        the_array = xr.DataArray(bands_numpy, coords=coords,dims=dims,name="openEODataChunk")
        return DataCube(the_array)


    def apply_tiles_spatiotemporal(self,function) -> ImageCollection:
        """
        Apply a function to a group of tiles with the same spatial key.
        :param function:
        :return:
        """

        #early compile to detect syntax errors
        compiled_code = compile(function,'UDF.py',mode='exec')

        def tilefunction(metadata:Metadata, openeo_metadata: CollectionMetadata, tiles:Tuple[gps.SpatialKey, List[Tuple[SpaceTimeKey, Tile]]]):
            tile_list = list(tiles[1])
            #sort by instant
            tile_list.sort(key=lambda tup: tup[0].instant)
            dates = map(lambda t: t[0].instant, tile_list)
            arrays = map(lambda t: t[1].cells, tile_list)
            multidim_array = np.array(list(arrays))

            extent = GeotrellisTimeSeriesImageCollection._mapTransform(metadata.layout_definition,tile_list[0][0])

            from openeo_udf.api.run_code import run_user_code
            from openeo_udf.api.datacube import DataCube
            #new UDF API available

            datacube:DataCube = GeotrellisTimeSeriesImageCollection._tile_to_datacube(
                multidim_array,
                extent=extent,
                bands_metadata=openeo_metadata.bands,
                start_times=pd.DatetimeIndex(dates)
            )

            data = UdfData({"EPSG": 900913}, [datacube])

            result_data = run_user_code(function,data)
            cubes = result_data.get_datacube_list()
            if len(cubes)!=1:
                raise ValueError("The provided UDF should return one datacube, but got: "+ str(cubes))
            result_array:xr.DataArray = cubes[0].array
            if 't' in result_array.dims:
                return [(SpaceTimeKey(col=tiles[0].col, row=tiles[0].row,instant=pd.Timestamp(timestamp)),
                  Tile(array_slice.values, CellType.FLOAT64, tile_list[0][1].no_data_value)) for timestamp, array_slice in result_array.groupby('t')]
            else:
                return [(SpaceTimeKey(col=tiles[0].col, row=tiles[0].row,instant=datetime.now()),
                  Tile(result_array.values, CellType.FLOAT64, tile_list[0][1].no_data_value))]




        def rdd_function(openeo_metadata: CollectionMetadata, rdd):
            floatrdd = rdd.convert_data_type(CellType.FLOAT64).to_numpy_rdd()
            grouped_by_spatial_key = floatrdd.map(lambda t: (gps.SpatialKey(t[0].col, t[0].row), (t[0], t[1]))).groupByKey()

            return gps.TiledRasterLayer.from_numpy_rdd(gps.LayerType.SPACETIME,
                                                       grouped_by_spatial_key.flatMap(
                                                    partial(tilefunction, rdd.layer_metadata, openeo_metadata)),
                                                       rdd.layer_metadata)
        from functools import partial
        return self.apply_to_levels(partial(rdd_function, self.metadata))


    def apply_tiles(self, function) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        #TODO apply .bands(bands)

        def tilefunction(metadata: Metadata, openeo_metadata: CollectionMetadata,
                         geotrellis_tile: Tuple[SpaceTimeKey, Tile]):

            key = geotrellis_tile[0]
            extent = GeotrellisTimeSeriesImageCollection._mapTransform(metadata.layout_definition,key)

            from openeo_udf.api.run_code import run_user_code
            from openeo_udf.api.datacube import DataCube

            datacube:DataCube = GeotrellisTimeSeriesImageCollection._tile_to_datacube(
                geotrellis_tile[1].cells,
                extent=extent,
                bands_metadata=openeo_metadata.bands
            )

            data = UdfData({"EPSG": 900913}, [datacube])

            result_data = run_user_code(function,data)
            cubes = result_data.get_datacube_list()
            if len(cubes)!=1:
                raise ValueError("The provided UDF should return one datacube, but got: "+ str(cubes))
            result_array:xr.DataArray = cubes[0].array
            print(result_array.dims)
            return (key,Tile(result_array.values, geotrellis_tile[1].cell_type,geotrellis_tile[1].no_data_value))


        def rdd_function(openeo_metadata: CollectionMetadata, rdd):
            return gps.TiledRasterLayer.from_numpy_rdd(rdd.layer_type,
                                                rdd.convert_data_type(CellType.FLOAT64).to_numpy_rdd().map(
                                                    partial(tilefunction, rdd.layer_metadata, openeo_metadata)),
                                                rdd.layer_metadata)
        from functools import partial
        return self.apply_to_levels(partial(rdd_function, self.metadata))

    def aggregate_time(self, temporal_window, aggregationfunction) -> Series :
        #group keys
        #reduce
        pass

    def aggregate_temporal(self, intervals:List,labels:List, reducer, dimension:str = 'temporal') -> 'ImageCollection' :
        """ Computes a temporal aggregation based on an array of date and/or time intervals.

            Calendar hierarchies such as year, month, week etc. must be transformed into specific intervals by the clients. For each interval, all data along the dimension will be passed through the reducer. The computed values will be projected to the labels, so the number of labels and the number of intervals need to be equal.

            If the dimension is not set, the data cube is expected to only have one temporal dimension.

            :param intervals: Temporal left-closed intervals so that the start time is contained, but not the end time.
            :param labels: Labels for the intervals. The number of labels and the number of groups need to be equal.
            :param reducer: A reducer to be applied on all values along the specified dimension. The reducer must be a callable process (or a set processes) that accepts an array and computes a single return value of the same type as the input values, for example median.
            :param dimension: The temporal dimension for aggregation. All data along the dimension will be passed through the specified reducer. If the dimension is not set, the data cube is expected to only have one temporal dimension.

            :return: An ImageCollection containing  a result for each time window
        """
        intervals_iso = list(map(lambda d:pd.to_datetime(d).strftime('%Y-%m-%dT%H:%M:%SZ'),intervals))
        labels_iso = list(map(lambda l:pd.to_datetime(l).strftime('%Y-%m-%dT%H:%M:%SZ'), labels))
        pysc = gps.get_spark_context()
        mapped_keys = self._apply_to_levels_geotrellis_rdd(
            lambda rdd,level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().mapInstantToInterval(rdd,intervals_iso,labels_iso))
        reducer = self._normalize_reducer(dimension, reducer)
        return mapped_keys.apply_to_levels(lambda rdd: rdd.aggregate_by_cell(reducer))

    def _aggregate_over_time_numpy(self, reducer: Callable[[Iterable[Tile]], Tile]) -> 'ImageCollection':
        """
        Aggregate over time.
        :param reducer: a function that reduces n Tiles to a single Tile
        :return:
        """
        def aggregate_temporally(layer):
            grouped_numpy_rdd = layer.to_spatial_layer().convert_data_type(CellType.FLOAT32).to_numpy_rdd().groupByKey()

            composite = grouped_numpy_rdd.mapValues(reducer)
            aggregated_layer = TiledRasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL, composite, layer.layer_metadata)
            return aggregated_layer

        return self.apply_to_levels(aggregate_temporally)


    @classmethod
    def __reproject_polygon(cls, polygon: Union[Polygon, MultiPolygon], srs, dest_srs):
        from shapely.ops import transform
        from functools import partial

        project = partial(
            pyproj.transform,
            pyproj.Proj(srs),  # source coordinate system
            pyproj.Proj(dest_srs))  # destination coordinate system

        return transform(project, polygon)  # apply projection

    def merge(self,other:'GeotrellisTimeSeriesImageCollection',overlaps_resolver:str=None):
        #we may need to align datacubes automatically?
        #other_pyramid_levels = {k: l.tile_to_layout(layout=self.pyramid.levels[k]) for k, l in other.pyramid.levels.items()}
        pysc = gps.get_spark_context()
        return self._apply_to_levels_geotrellis_rdd(
            lambda rdd, level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().mergeCubes(rdd, other.pyramid.levels[
                level].srdd.rdd(), overlaps_resolver))

    def mask(self, polygon: Union[Polygon, MultiPolygon]= None, srs="EPSG:4326",rastermask:'ImageCollection'=None,replacement=None) -> 'ImageCollection':
        if polygon is not None:
            max_level = self.pyramid.levels[self.pyramid.max_zoom]
            layer_crs = max_level.layer_metadata.crs
            reprojected_polygon = GeotrellisTimeSeriesImageCollection.__reproject_polygon(polygon,"+init="+srs,layer_crs)
            #TODO should we warn when masking generates an empty collection?
            return self.apply_to_levels(lambda rdd: rdd.mask(
                reprojected_polygon,
                 partition_strategy=None,
                 options=gps.RasterizerOptions()))
        elif rastermask is not None:
            pysc = gps.get_spark_context()
            #mask needs to be the same layout as this layer
            mask_pyramid_levels = {k: l.tile_to_layout(layout=self.pyramid.levels[k]) for k, l in rastermask.pyramid.levels.items()}
            return self._apply_to_levels_geotrellis_rdd(
                lambda rdd,level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().rasterMask(rdd, mask_pyramid_levels[level].srdd.rdd(),replacement))
        else:
            raise AttributeError("mask process: either a polygon or a rastermask should be provided.")

    def apply_kernel(self,kernel,factor):

        pysc = gps.get_spark_context()

        #converting a numpy array into a geotrellis tile seems non-trivial :-)
        kernel_tile = Tile.from_numpy_array(kernel, no_data_value=None)
        rdd = pysc.parallelize([(gps.SpatialKey(0,0), kernel_tile)])
        metadata = {'cellType': str(kernel.dtype),
                    'extent': {'xmin': 0.0, 'ymin': 0.0, 'xmax': 1.0, 'ymax': 1.0},
                    'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                    'bounds': {
                        'minKey': {'col': 0, 'row': 0},
                        'maxKey': {'col': 0, 'row': 0}},
                    'layoutDefinition': {
                        'extent': {'xmin': 0.0, 'ymin': 0.0, 'xmax': 1.0, 'ymax': 1.0},
                        'tileLayout': {'tileCols': 5, 'tileRows': 5, 'layoutCols': 1, 'layoutRows': 1}}}
        geopyspark_layer = TiledRasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL, rdd, metadata)
        geotrellis_tile = geopyspark_layer.srdd.rdd().collect()[0]._2().band(0)

        if self.pyramid.layer_type == gps.LayerType.SPACETIME:
            result_collection = self._apply_to_levels_geotrellis_rdd(
                lambda rdd, level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().apply_kernel_spacetime(rdd, geotrellis_tile))
        else:
            result_collection = self._apply_to_levels_geotrellis_rdd(
                lambda rdd, level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().apply_kernel_spatial(rdd,geotrellis_tile))
        if(factor != 1.0):
            result_collection.pyramid = result_collection.pyramid * factor
        return result_collection

    def resample_spatial(
            self,
            resolution: Union[float, Tuple[float, float]],
            projection: Union[int, str] = None,
            method: str = 'near',
            align: str = 'upper-left'
    ):
        """
        https://open-eo.github.io/openeo-api/v/0.4.0/processreference/#resample_spatial
        :param resolution:
        :param projection:
        :param method:
        :param align:
        :return:
        """

        # TODO: use align

        resample_method = {
            'bilinear': gps.ResampleMethod.BILINEAR,
            'average': gps.ResampleMethod.AVERAGE,
            'cubic': gps.ResampleMethod.CUBIC_CONVOLUTION,
            'cubicspline': gps.ResampleMethod.CUBIC_SPLINE,
            'lanczos': gps.ResampleMethod.LANCZOS,
            'mode': gps.ResampleMethod.MODE,
            'max': gps.ResampleMethod.MAX,
            'min': gps.ResampleMethod.MIN,
            'med': gps.ResampleMethod.MEDIAN,
        }.get(method, gps.ResampleMethod.NEAREST_NEIGHBOR)

        #IF projection is defined, we need to warp
        if projection is not None:
            reprojected = self.apply_to_levels(lambda layer: gps.TiledRasterLayer(
                layer.layer_type, layer.srdd.reproject(str(projection), resample_method, None)
            ))
            return reprojected
        elif resolution != 0.0:

            max_level = self.pyramid.levels[self.pyramid.max_zoom]
            extent = max_level.layer_metadata.layout_definition.extent

            if projection is not None:
                extent = self._reproject_extent(
                    max_level.layer_metadata.crs, projection, extent.xmin, extent.ymin, extent.xmax, extent.ymax
                )

            width = extent.xmax - extent.xmin
            height = extent.ymax - extent.ymin

            nbTilesX = width / (256 * resolution)
            nbTilesY = height / (256 * resolution)

            exactTileSizeX = width/(resolution * math.ceil(nbTilesX))
            exactNbTilesX = width/(resolution * exactTileSizeX)

            exactTileSizeY = height / (resolution * math.ceil(nbTilesY))
            exactNbTilesY = height / (resolution * exactTileSizeY)


            newLayout = gps.LayoutDefinition(extent=extent,tileLayout=gps.TileLayout(int(exactNbTilesX),int(exactNbTilesY),int(exactTileSizeX),int(exactTileSizeY)))

            if(projection is not None):
                resampled = max_level.tile_to_layout(newLayout,target_crs=projection, resample_method=resample_method)
            else:
                resampled = max_level.tile_to_layout(newLayout,resample_method=resample_method)

            pyramid = Pyramid({0:resampled})
            return GeotrellisTimeSeriesImageCollection(pyramid, self._service_registry,
                                                       metadata=self.metadata)._with_band_index(self._band_index)
            #return self.apply_to_levels(lambda layer: layer.tile_to_layout(projection, resample_method))
        return self

    def linear_scale_range(self, input_min, input_max, output_min, output_max) -> 'ImageCollection':
        """ Color stretching
            :param input_min: Minimum input value
            :param input_max: Maximum input value
            :param output_min: Minimum output value
            :param output_max: Maximum output value
            :return An ImageCollection instance
        """
        rescaled = self.apply_to_levels(lambda layer: layer.normalize(output_min, output_max, input_min, input_max))
        output_range = output_max - output_min
        if output_range >1 and type(output_min) == int and type(output_max) == int:
            if output_range < 254 and output_min >= 0:
                rescaled = rescaled.apply_to_levels(lambda layer: layer.convert_data_type(gps.CellType.UINT8,255))
            elif output_range < 65535 and output_min >= 0:
                rescaled = rescaled.apply_to_levels(lambda layer: layer.convert_data_type(gps.CellType.UINT16))
        return rescaled

    def timeseries(self, x, y, srs="EPSG:4326") -> Dict:
        max_level = self.pyramid.levels[self.pyramid.max_zoom]
        (x_layer,y_layer) = pyproj.transform(pyproj.Proj(init=srs),pyproj.Proj(max_level.layer_metadata.crs),x,y)
        points = [
            Point(x_layer, y_layer),
        ]
        values = max_level.get_point_values(points)
        result = {}
        if isinstance(values[0][1],List):
            values = values[0][1]
        for v in values:
            if isinstance(v,float):
                result["NoDate"]=v
            elif "isoformat" in dir(v[0]):
                result[v[0].isoformat()]=v[1]
            elif v[0] is None:
                #empty timeseries
                pass
            else:
                print("unexpected value: "+str(v))

        return result

    def zonal_statistics(self, regions, func) -> AggregatePolygonResult:
        # TODO eliminate code duplication
        def insert_timezone(instant):
            return instant.replace(tzinfo=pytz.UTC) if instant.tzinfo is None else instant

        from_vector_file = isinstance(regions, str)
        multiple_geometries = from_vector_file or isinstance(regions, GeometryCollection)

        if func == 'histogram' or func == 'sd':
            highest_level = self.pyramid.levels[self.pyramid.max_zoom]
            layer_metadata = highest_level.layer_metadata

            scala_data_cube = highest_level.srdd.rdd()

            polygon_wkts = [str(ob) for ob in regions] if multiple_geometries else [str(regions)]
            polygons_srs = 'EPSG:4326'
            from_date = insert_timezone(layer_metadata.bounds.minKey.instant)
            to_date = insert_timezone(layer_metadata.bounds.maxKey.instant)

            if func == 'histogram':
                if multiple_geometries:
                    implementation = self._compute_stats_geotrellis().compute_histograms_time_series_from_datacube
                    polygon_wkts = (str(ob) for ob in regions)
                else:
                    implementation = self._compute_stats_geotrellis().compute_histogram_time_series_from_datacube
                    polygon_wkts = str(regions)
            elif func == 'sd':
                implementation = self._compute_stats_geotrellis().compute_sd_time_series_from_datacube
            else:
                raise ValueError(func)

            stats = implementation(
                scala_data_cube,
                polygon_wkts,
                polygons_srs,
                from_date.isoformat(),
                to_date.isoformat(),
                self._band_index
            )

            return AggregatePolygonResult(
                timeseries=self._as_python(stats),
                regions=regions if multiple_geometries else GeometryCollection([regions]),
            )
        elif func == 'median':
            highest_level = self.pyramid.levels[self.pyramid.max_zoom]
            layer_metadata = highest_level.layer_metadata

            scala_data_cube = highest_level.srdd.rdd()

            from_date = insert_timezone(layer_metadata.bounds.minKey.instant)
            to_date = insert_timezone(layer_metadata.bounds.maxKey.instant)

            if from_vector_file:
                stats = self._compute_stats_geotrellis().compute_median_time_series_from_datacube(
                    scala_data_cube,
                    regions,
                    from_date.isoformat(),
                    to_date.isoformat(),
                    self._band_index
                )
            else:
                polygon_wkts = [str(ob) for ob in regions] if multiple_geometries else [str(regions)]
                polygons_srs = 'EPSG:4326'

                stats = self._compute_stats_geotrellis().compute_median_time_series_from_datacube(
                    scala_data_cube,
                    polygon_wkts,
                    polygons_srs,
                    from_date.isoformat(),
                    to_date.isoformat(),
                    self._band_index
                )

            return AggregatePolygonResult(
                timeseries=self._as_python(stats),
                regions=regions if multiple_geometries else GeometryCollection([regions]),
            )
        else:  # defaults to mean, historically
            if multiple_geometries:
                highest_level = self.pyramid.levels[self.pyramid.max_zoom]
                layer_metadata = highest_level.layer_metadata
                scala_data_cube = highest_level.srdd.rdd()
                polygons_srs = 'EPSG:4326'
                from_date = insert_timezone(layer_metadata.bounds.minKey.instant)
                to_date = insert_timezone(layer_metadata.bounds.maxKey.instant)

                with tempfile.NamedTemporaryFile(suffix=".json.tmp") as temp_file:
                    if from_vector_file:
                        self._compute_stats_geotrellis().compute_average_timeseries_from_datacube(
                            scala_data_cube,
                            regions,
                            from_date.isoformat(),
                            to_date.isoformat(),
                            self._band_index,
                            temp_file.name
                        )

                    else:
                        self._compute_stats_geotrellis().compute_average_timeseries_from_datacube(
                            scala_data_cube,
                            [str(ob) for ob in regions],
                            polygons_srs,
                            from_date.isoformat(),
                            to_date.isoformat(),
                            self._band_index,
                            temp_file.name
                        )

                    with open(temp_file.name, encoding='utf-8') as f:
                        timeseries = json.load(f)
                return AggregatePolygonResult(
                    timeseries=timeseries,
                    regions=regions,
                )
            else:
                return AggregatePolygonResult(
                    timeseries=self.polygonal_mean_timeseries(regions),
                    regions=GeometryCollection([regions]),
                )

    def _compute_stats_geotrellis(self):
        jvm = gps.get_spark_context()._gateway.jvm
        accumulo_instance_name = 'hdp-accumulo-instance'
        return jvm.org.openeo.geotrellis.ComputeStatsGeotrellisAdapter(self._zookeepers(), accumulo_instance_name)

    def _zookeepers(self):
        return ','.join(ConfigParams().zookeepernodes)

    # FIXME: define this somewhere else?
    def _as_python(self, java_object):
        """
        Converts Java collection objects retrieved from Py4J to their Python counterparts, recursively.
        :param java_object: a JavaList or JavaMap
        :return: a Python list or dictionary, respectively
        """

        from py4j.java_collections import JavaList, JavaMap

        if isinstance(java_object, JavaList):
            return [self._as_python(elem) for elem in list(java_object)]

        if isinstance(java_object, JavaMap):
            return {self._as_python(key): self._as_python(value) for key, value in dict(java_object).items()}

        return java_object

    def polygonal_mean_timeseries(self, polygon: Union[Polygon, MultiPolygon]) -> Dict:
        max_level = self.pyramid.levels[self.pyramid.max_zoom]
        layer_crs = max_level.layer_metadata.crs
        reprojected_polygon = GeotrellisTimeSeriesImageCollection.__reproject_polygon(polygon, "+init=EPSG:4326" ,layer_crs)

        #TODO somehow mask function was masking everything, while the approach with direct timeseries computation did not have issues...
        masked_layer = max_level.mask(reprojected_polygon)

        no_data = masked_layer.layer_metadata.no_data_value

        def combine_cells(acc: List[Tuple[int, int]], tile) -> List[Tuple[int, int]]:  # [(sum, count)]
            n_bands = len(tile.cells)

            if not acc:
                acc = [(0, 0)] * n_bands

            for i in range(n_bands):
                grid = tile.cells[i]

                # special treatment for a UDF layer (NO_DATA is nan so every value, including nan, is not equal to nan)
                without_no_data = (~np.isnan(grid)) & (grid != no_data)

                sum = grid[without_no_data].sum()
                count = without_no_data.sum()

                acc[i] = acc[i][0] + sum, acc[i][1] + count

            return acc

        def combine_values(l1: List[Tuple[int, int]], l2: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
            for i in range(len(l2)):
                l1[i] = l1[i][0] + l2[i][0], l1[i][1] + l2[i][1]

            return l1

        polygon_mean_by_timestamp = masked_layer.to_numpy_rdd() \
            .map(lambda pair: (pair[0].instant, pair[1])) \
            .aggregateByKey([], combine_cells, combine_values)

        def to_mean(values: Tuple[int, int]) -> float:
            sum, count = values
            return sum / count

        collected = polygon_mean_by_timestamp.collect()
        return {timestamp.isoformat(): [[to_mean(v) for v in values]] for timestamp, values in collected}

    def download(self,outputfile:str, **format_options) -> str:
        """Extracts a geotiff from this image collection."""
        #geotiffs = self.rdd.merge().to_geotiff_rdd(compression=gps.Compression.DEFLATE_COMPRESSION).collect()
        filename = outputfile
        if outputfile is None:
            _, filename = tempfile.mkstemp(suffix='.oeo-gps-dl')
        else:
            filename = outputfile
        spatial_rdd = self.pyramid.levels[self.pyramid.max_zoom]
        if spatial_rdd.layer_type != gps.LayerType.SPATIAL:
            spatial_rdd = spatial_rdd.to_spatial_layer()

        tiled = format_options.get("format", "GTiff").upper() == "GTIFF" and format_options.get("tiled", False)

        catalog = (format_options.get("format", "GTiff").upper() == "GTIFF" and
                 format_options.get("parameters", {}).get("catalog", False))

        xmin, ymin, xmax, ymax = format_options.get('left'), format_options.get('bottom'),\
                                 format_options.get('right'), format_options.get('top')

        if xmin and ymin and xmax and ymax:
            srs = format_options.get('srs', 'EPSG:4326')

            src_crs = "+init=" + srs
            dst_crs = spatial_rdd.layer_metadata.crs
            crop_bounds = self._reproject_extent(src_crs, dst_crs, xmin, ymin, xmax, ymax)
        else:
            crop_bounds = None

        if catalog:
            self._save_on_executors(spatial_rdd, filename)
        elif tiled:
            self._save_stitched_tiled(spatial_rdd, filename)
        else:
            self._save_stitched(spatial_rdd, filename, crop_bounds)

        return filename

    def _reproject_extent(self, src_crs, dst_crs, xmin, ymin, xmax, ymax):
        src_proj = pyproj.Proj(src_crs)
        dst_proj = pyproj.Proj(dst_crs)

        def reproject_point(x, y):
            return pyproj.transform(
                src_proj,
                dst_proj,
                x, y
            )

        reprojected_xmin, reprojected_ymin = reproject_point(xmin, ymin)
        reprojected_xmax, reprojected_ymax = reproject_point(xmax, ymax)
        crop_bounds = \
            Extent(xmin=reprojected_xmin, ymin=reprojected_ymin, xmax=reprojected_xmax, ymax=reprojected_ymax)
        return crop_bounds

    def _save_on_executors(self, spatial_rdd: gps.TiledRasterLayer, path):
        geotiff_rdd = spatial_rdd.to_geotiff_rdd(
            storage_method=gps.StorageMethod.TILED,
            compression=gps.Compression.DEFLATE_COMPRESSION
        )

        basedir = pathlib.Path(str(path) + '.catalogresult')
        basedir.mkdir(parents=True, exist_ok=True)

        def write_tiff(item):
            key, data = item
            path = basedir / '{c}-{r}.tiff'.format(c=key.col, r=key.row)
            with path.open('wb') as f:
                f.write(data)

        geotiff_rdd.foreach(write_tiff)
        tiffs = [str(path.absolute()) for path in basedir.glob('*.tiff')]

        _log.info("Merging results {t!r}".format(t=tiffs))
        merge_args = ["gdal_merge.py", "-o", path, "-of", "GTiff", "-co", "COMPRESS=DEFLATE", "-co", "TILED=TRUE"]
        merge_args += tiffs
        _log.info("Executing: {a!r}".format(a=merge_args))
        subprocess.check_call(merge_args, shell=False, env={"GDAL_CACHEMAX": "1024"})


    def _save_stitched(self, spatial_rdd, path, crop_bounds=None):
        jvm = gps.get_spark_context()._gateway.jvm

        max_compression = jvm.geotrellis.raster.io.geotiff.compression.DeflateCompression(9)

        if crop_bounds:
            jvm.org.openeo.geotrellis.geotiff.package.saveStitched(spatial_rdd.srdd.rdd(), path, crop_bounds._asdict(),
                                                                   max_compression)
        else:
            jvm.org.openeo.geotrellis.geotiff.package.saveStitched(spatial_rdd.srdd.rdd(), path, max_compression)

    def _save_stitched_tiled(self, spatial_rdd, filename):
        import rasterio as rstr
        from affine import Affine
        import rasterio._warp as rwarp
        from math import log

        max_level = self.pyramid.levels[self.pyramid.max_zoom]

        spatial_rdd = spatial_rdd.persist()

        sorted_keys = sorted(spatial_rdd.collect_keys())

        upper_left_coords = GeotrellisTimeSeriesImageCollection._mapTransform(max_level.layer_metadata.layout_definition, sorted_keys[0])
        lower_right_coords = GeotrellisTimeSeriesImageCollection._mapTransform(max_level.layer_metadata.layout_definition, sorted_keys[-1])

        data = spatial_rdd.stitch()

        bands, w, h = data.cells.shape
        nodata = max_level.layer_metadata.no_data_value
        dtype = data.cells.dtype
        ex = Extent(xmin=upper_left_coords.left, ymin=lower_right_coords.bottom, xmax=lower_right_coords.right, ymax=upper_left_coords.top)
        cw, ch = (ex.xmax - ex.xmin) / w, (ex.ymax - ex.ymin) / h
        overview_level = int(log(w) / log(2) - 8)

        with rstr.io.MemoryFile() as memfile, open(filename, 'wb') as f:
            with memfile.open(driver='GTiff',
                              count=bands,
                              width=w,
                              height=h,
                              transform=Affine(cw, 0.0, ex.xmin,
                                               0.0, -ch, ex.ymax),
                              crs=rstr.crs.CRS.from_proj4(spatial_rdd.layer_metadata.crs),
                              nodata=nodata,
                              dtype=dtype,
                              compress='lzw',
                              tiled=True) as mem:
                windows = list(mem.block_windows(1))
                for _, w in windows:
                    segment = data.cells[:, w.row_off:(w.row_off + w.height), w.col_off:(w.col_off + w.width)]
                    mem.write(segment, window=w)
                    mask_value = np.all(segment != nodata, axis=0).astype(np.uint8) * 255
                    mem.write_mask(mask_value, window=w)

                    overviews = [2 ** j for j in range(1, overview_level + 1)]
                    mem.build_overviews(overviews, rwarp.Resampling.nearest)
                    mem.update_tags(ns='rio_oveview', resampling=rwarp.Resampling.nearest.value)

            while True:
                chunk = memfile.read(8192)
                if not chunk:
                    break

                f.write(chunk)

    def _proxy_tms(self,tms):
        if ConfigParams().is_ci_context:
            return tms.url_pattern
        else:
            host = tms.host
            port = tms.port
            self._proxy(host, port)
            url = "http://openeo.vgt.vito.be/tile/{z}/{x}/{y}.png"
            return url

    def _proxy(self, host, port):
        from kazoo.client import KazooClient
        zk = KazooClient(hosts=self._zookeepers())
        zk.start()
        zk.ensure_path("discovery/services/openeo-viewer-test")
        # id = uuid.uuid4()
        # print(id)
        id = 0
        zk.ensure_path("discovery/services/openeo-viewer-test/" + str(id))
        zk.set("discovery/services/openeo-viewer-test/" + str(id), str.encode(json.dumps(
            {"name": "openeo-viewer-test", "id": str(id), "address": host, "port": port, "sslPort": None,
             "payload": None, "registrationTimeUTC": datetime.utcnow().strftime('%s'), "serviceType": "DYNAMIC"})))
        zk.stop()
        zk.close()

    def tiled_viewing_service(self, service_type: str, process_graph: dict, post_data: dict = {}) -> ServiceMetadata:
        service_id = str(uuid.uuid4())

        if service_type.lower() == "tms":
            if self.pyramid.layer_type != gps.LayerType.SPATIAL:
                spatial_rdd = self.apply_to_levels(lambda rdd: rdd.to_spatial_layer())
            print("Creating persisted pyramid.")
            spatial_rdd = spatial_rdd.apply_to_levels(lambda rdd: rdd.partitionBy(gps.SpatialPartitionStrategy(num_partitions=40,bits=2)).persist())
            print("Pyramid ready.")

            pyramid = spatial_rdd.pyramid
            def render_rgb(tile):
                import numpy as np
                from PIL import Image
                rgba = np.dstack([tile.cells[0] * (255.0 / 2000.0), tile.cells[1] * (255.0 / 2000.0),
                                  tile.cells[2] * (255.0 / 2000.0)]).astype('uint8')
                img = Image.fromarray(rgba, mode='RGB')
                return img

            # greens = color.get_colors_from_matplotlib(ramp_name="YlGn")
            greens = gps.ColorMap.build(breaks=[x / 100.0 for x in range(0, 100)], colors="YlGn")

            if(self.tms is None):
                self.tms = TMS.build(source=pyramid,display = greens)
                self.tms.bind(host="0.0.0.0",requested_port=0)

            url = self._proxy_tms(self.tms)
            level_bounds = {level: tiled_raster_layer.layer_metadata.bounds for level, tiled_raster_layer in
                            pyramid.levels.items()}
            # TODO handle cleanup of tms'es
            return ServiceMetadata(
                id=service_id,
                process={"process_graph": process_graph},
                url=url,
                type="TMS",
                enabled=True,
                attributes={"bounds": level_bounds},
                created=datetime.utcnow(),
            )
        else:

            pysc = gps.get_spark_context()

            random_port = 0
            wmts_base_url = os.getenv('WMTS_BASE_URL_PATTERN', 'http://openeo.vgt.vito.be/openeo/services/%s') % service_id

            # TODO: instead of storing in self, keep in a registry to allow cleaning up (wmts.stop)
            wmts = pysc._jvm.be.vito.eodata.gwcgeotrellis.wmts.WMTSServer.createServer(random_port, wmts_base_url)
            _log.info('Created WMTSServer: {w!s} ({u!s}, {p!r})'.format(w=wmts, u=wmts.getURI(), p=wmts.getPort()))

            # TODO: configuration should come from post_data["configuration"] (see API spec)
            if "style" in post_data:
                max_zoom = self.pyramid.max_zoom
                min_zoom = min(self.pyramid.levels.keys())
                reduced_resolution = max(min_zoom,max_zoom-4)
                if reduced_resolution not in self.pyramid.levels:
                    reduced_resolution = min_zoom
                histogram = self.pyramid.levels[reduced_resolution].get_histogram()
                matplotlib_name = post_data.get("style").get("colormap","YlGn")

                #color_map = gps.ColorMap.from_colors(breaks=[x for x in range(0,250)], color_list=gps.get_colors_from_matplotlib("YlGn"))
                color_map = gps.ColorMap.build(histogram, matplotlib_name)
                srdd_dict = {k: v.srdd.rdd() for k, v in self.pyramid.levels.items()}
                wmts.addPyramidLayer("RDD", srdd_dict,color_map.cmap)
            else:
                srdd_dict = {k: v.srdd.rdd() for k, v in self.pyramid.levels.items()}
                wmts.addPyramidLayer("RDD", srdd_dict)

            import socket
            # TODO what is this host logic about?
            host = [l for l in
                              ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1],
                               [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in
                                 [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]])
                              if l][0][0]

            service_metadata = ServiceMetadata(
                id=service_id,
                # TODO: can process graph be extracted from `self` instead of passing it explicitly?
                process={"process_graph": process_graph},
                url=wmts_base_url + "/service/wmts",
                type="WMTS",
                enabled=True,
                # TODO: fill in attributes?
                attributes={},
                created=datetime.utcnow(),
                # TODO: fill in more fields?
            )

            self._service_registry.register(SecondaryService(
                service_metadata=service_metadata,
                host=host, port=wmts.getPort(), server=wmts
            ))

            return service_metadata

    def ndvi(self, name: str = "ndvi") -> 'ImageCollection':
        try:
            red_index, = [i for i, b in enumerate(self.metadata.bands) if b.common_name == 'red']
            nir_index, = [i for i, b in enumerate(self.metadata.bands) if b.common_name == 'nir']
        except ValueError:
            raise ValueError("Failed to detect 'red' and 'nir' bands")

        reduce_graph = {
            "red": {
                "process_id": "array_element", "result": False,
                "arguments": {"data": {"from_argument": "data"}, "index": red_index}
            },
            "nir": {
                "process_id": "array_element", "result": False,
                "arguments": {"data": {"from_argument": "data"}, "index": nir_index}
            },
            "nirminusred": {
                "process_id": "subtract", "result": False,
                "arguments": {
                    "data": [{"from_node": "nir"}, {"from_node": "red"}]
                }
            },
            "nirplusred": {
                "process_id": "sum", "result": False,
                "arguments": {
                    "data": [{"from_node": "nir"}, {"from_node": "red"}]
                }
            },
            "ndvi": {
                "process_id": "divide", "result": True,
                "arguments": {
                    "data": [{"from_node": "nirminusred"}, {"from_node": "nirplusred"}]
                }
            },
        }

        from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
        visitor = GeotrellisTileProcessGraphVisitor()
        # TODO: how to set name (argument `name`) of newly created band?
        return self.reduce_bands(visitor.accept_process_graph(reduce_graph))
