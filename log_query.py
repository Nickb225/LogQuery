import datetime
import logging
import pandas
import pprint
from pathlib import Path # for local
class LogQuery():
    def __init__(self,server_paths):
        # Pandas dataframe is fast and cheap.
        data = []
        self.servers = server_paths
        for server_name,path in self.servers.items():
            with open(Path(path)) as f:
                # single threaded. Can multithread later they're all independent.
                for line in f.readlines(): # if files are too large, can chunk w/ a BUF_SIZE param in readlines and continue reading.
                    res = self._parse_log(line)
                    if res:
                        data.append({**res,"server" : server_name}) # append outside of the loop to avoid unnecessary pass.


        # TODO: Can probably spend quite a bit of time optimizing this - we do 0 caching of finished datasources
        # We also create the index after the creation, then sort.
        #
        self.datastore = pandas.DataFrame(data)
        self.datastore = self.datastore.astype({
            "datetime" : 'datetime64[ns]',
            "severity" : int,
            "server" : str,
            "text" : str
        })
        self.datastore = self.datastore.set_index(["datetime"])
        self.datastore.sort_index(inplace=True, ascending=True)



    # custom parser - putting this into the text so that I multithread this later as an apply if I want.
    def _parse_log(self,line):
        datetime = None
        severity = None
        text = None

        ## A valid line looks like [02/28/2020 5:20:55.17][info] Opening database “my_db7” for write.
        # Could be faster in future to just find first two "]"

        line_parts = line.split("]")
        if len(line_parts) <= 1:
            logging.warning("Innacurate formatted line found in log - skipping")
            return
        # Parse date
        try:
            datetime = pandas.to_datetime(line_parts[0][1:])
        except:
            logging.warning(f"Innacurate time found in log - skipping {line_parts[0]}")
            return # fail.

        # parse severity
        try:
            severity = logging._checkLevel(line_parts[1][1:].upper())
        except:
            logging.warning(f"Unsupported logging level found - skipping {severity}")

        # Little weird because we used string split - can have ] in the index.
        text = "]".join(line_parts[2:]).strip()

        return {
            "datetime" : datetime,
            "severity" : severity,
            "text" : text
        }
    # nice helper function for debugging.
    def get_pandas(self):
        return self.datastore
    # Begin scraping the logs
    def query(self,
              start_time = None,
              end_time = None,
              entries = 100,
              keys=None,
              min_severity = logging.WARN
              ):

        res = self.datastore
        # If we get large enough data - we'll probably want to swap over to a dask dataframe for lazy evaluation
        # We indexed by date - so am able to use the index.
        # TODO: Check for our index  we expect - else this could fail.
        if start_time is not None:
            res = res.loc[start_time:]
        if end_time is not None:
            res = res.loc[:end_time]

        if keys:
            res = res[res.server in keys]

        res = res[res.severity >= min_severity]

        # put this last so we don't limit too early
        res = res.head(n=entries)

        return res


if __name__ == '__main__':
    pandas.set_option('display.max_rows', None)
    pandas.set_option('display.max_columns', None)
    pandas.set_option('display.width', None)
    pandas.set_option('display.max_colwidth', -1)
    A = LogQuery({"server1": "/Users/nickbenthem/scratch/logs_1.log"})
    print(A.query(start_time=datetime.datetime(year=2019,month=4,day=6),entries=3))