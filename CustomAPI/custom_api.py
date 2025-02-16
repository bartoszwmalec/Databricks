from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
import re

# https://learn.microsoft.com/en-us/azure/databricks/pyspark/datasources
# https://community.databricks.com/t5/technical-blog/enhancing-the-new-pyspark-custom-data-sources-streaming-api/ba-p/75538


class CustomAPI(DataSource):
    """
    Custom API support for loading Datasources not supported (at the time of this development) by the Fivetran
    """

    @classmethod
    def name(cls):
        return "Custom_API_Source"

    def schema(self):
        return "id int, name string, email string, body string"

    def streamReader(self, schema: StructType):
        return CommentsAPIStreamReader(schema, self.options)
    
class CommentsAPIStreamReader(DataSourceStreamReader):
    def __init__(self, schema, options):
        self.api_url = options.get("api_url")
        self.progress_path = options.get("progress_path")
        self.rows_per_batch = options.get("rows_per_batch",10)
        self.url = f"https://{workspace_url}/api/2.0/fs/files{self.progress_path}progress.json"
        self.token = token
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
        return [RangePartition(start["offset"], end["offset"])]

    def commit(self, end: dict):

        """
        This is invoked when the query has finished processing data before end offset. This
        can be used to clean up the resource.
        """
        self._save_progress()

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
          yield (r['id'],r['name'],r['email'], r['body'])
