from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.types import StructType
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Sequence, Tuple, Type, Union, TYPE_CHECKING
# from pyspark.sql.functions import spark_partition_id
import requests
from datetime import datetime
from pyspark.errors import PySparkNotImplementedError

"""
Requirements:
PySpark custom data sources are in Public Preview in Databricks Runtime 15.2 and above. Streaming support is available in Databricks Runtime 15.3 and above.

References:
# https://www.youtube.com/watch?v=Q7GdpqYlBiI "Introducing the New Python Data Source API for Apache Spark™"
# https://github.com/apache/spark/blob/0d7c07047a628bd42eb53eb49935f5e3f81ea1a1/python/pyspark/sql/datasource.py
# https://learn.microsoft.com/en-us/azure/databricks/pyspark/datasources
# https://community.databricks.com/t5/technical-blog/enhancing-the-new-pyspark-custom-data-sources-streaming-api/ba-p/75538

Problems:
1. Where to define a method for token retrieval?
"""

class CustomAPI(DataSource):
    """
    Custom API support for loading Datasources not supported (at the time of this development) by the Fivetran
    """

    @classmethod
    def name(cls):
        # Returned name would be used as a format name in commands like "spark.read.format(CustomAPI)" or "spark.write.format(CustomAPI)"
        return "CustomAPI"

    def schema(self):
        # NotYetImplemented - self-discovery schema during data load using built-in "schema_of_json" function, ie. schema = f'schema_of_json(response)'
        # dict[str, str] | StructType 
        '''
        self.schema: StructType = schema
        return "id int, name string, email string, body string"
            raise PySparkNotImplementedError(
            error_class="NOT_IMPLEMENTED",
            message_parameters={"feature": "schema"},
        )
        '''
        return "id int, name string, email string, body string"

    def streamReader(self, schema: StructType):
        return CustomAPIStreamReader(schema, self.options)
    
    def streamWriter(self, schema: StructType):
        return CustomAPIStreamWriter(schema, self.options)

    def Reader(self, schema: StructType):
        return CustomAPIReader(schema, self.options)
    
    def streamWriter(self, schema: StructType):
        return CustomAPIWriter(schema, self.options)

class CustomAPIReader(DataSourceReader):
    pass
    # raise PySparkNotImplementedError(
    #         error_class="NOT_IMPLEMENTED",
    #         message_parameters={"feature": "schema"},
    #     )

class CustomAPIWriter(DataSourceWriter):
    pass
    # raise PySparkNotImplementedError(
    #         error_class="NOT_IMPLEMENTED",
    #         message_parameters={"feature": "schema"},
    #     )

class CustomAPIStreamWriter(DataSourceStreamWriter):
    pass
    # raise PySparkNotImplementedError(
    #         error_class="NOT_IMPLEMENTED",
    #         message_parameters={"feature": "schema"},
    #     )

class CustomAPIStreamReader(DataSourceStreamReader):
    def __init__(self, schema, options, protocol: str = "https", method: str = "GET"):
        """
        Initializer of the class. Collects all required parameters.

        Complete URI (ie. "https://${DATABRICKS_HOST}/api/2.0/clusters/get") consists of :
        * Protocol: default "https"
        * Host: host-name - literally, everything between "slash" after the protocol name and before the "slash" before API path
        * Path: target API endoint that we would like to call
        * Query: query to be pushed agains the give URI to limit the number of records; Parameter present after the "?" sign
        """
        self.method = options.get("method", method)
        self.protocol = options.get("protocol", protocol)
        self.host = options.get("host")
        self.path = options.get("path")
        self.checkpoint_path = options.get("checkpoint_path")
        self.rows_per_batch = options.get("rows_per_batch",10)
        self.uri = f"{protocol}://{host}/{path}" # self.uri = f"{protocol}://{host}/{path}?{query}"
        self.token = token
        # self.query = options.get("query", query)
        self._load_progress()
        
    def initialOffset(self) -> dict:
        """
        Returns the initial start offset of the reader.
        """
        return {"offset": 0}
      
    def _load_progress(self):
      headers = {
          "Authorization": f"Bearer {self.token}",
      }
      response = requests.get(self.url, headers=headers)
      
      current = response.json().get('current',0)
      self.current = current
      
    def _save_progress(self):
      url = f"{self.url}?overwrite=true"

      headers = {
          "Authorization": f"Bearer {self.token}",
          "Content-Type": "application/json"
      }
      data = json.dumps({"current": self.current}) 
      response = requests.put(url, headers=headers, data=data)


    def latestOffset(self) -> dict:
        """
        Returns the current latest offset that the next microbatch will read to.
        """
        self.current += self.rows_per_batch
        return {"offset": self.current}

    def partitions(self, start: dict, end: dict):
      
        """
        Plans the partitioning of the current microbatch defined by start and end offset. It
        needs to return a sequence of :class:`InputPartition` objects.
        """

        # RangePartition is part of InputPartition
        return [RangePartition(start["offset"], end["offset"])]

    def commit(self, end: dict):

        """
        This is invoked when the query has finished processing data before end offset. This
        can be used to clean up the resource.
        """
        self._save_progress()

    @abstractmethod 
    def read(self, partition) -> Iterator[Tuple]:
        """
        Takes a partition as an input and reads an iterator of tuples from the data source.
        """
        start, end = partition.start, partition.end
        params = {
          "_start": start,
          "_limit": self.rows_per_batch
        }
        response = requests.get(self.api_url, params=params)
        for r in response.json():
        #   for schema = "id int, name string, email string, body string"
          '''
          # Functionality to receive 
          column_names = [field.name for field in schema.fields]
          if not column_names:
            return ""

          formatted_columns = [f"r['{col}']" for col in column_names]
          yield_field = return ", ".join(formatted_columns)
          yield (f'{yield_field})
          '''
          yield (r['id'],r['name'],r['email'], r['body'])
