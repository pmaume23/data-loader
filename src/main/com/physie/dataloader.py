from pyspark.sql import SparkSession

class Dataloader:

    #Create a spark session
    spark = SparkSession.builder.appName("FileReader").getOrCreate()
    def __init__(self,file_format,header,inferschema,path):
        self.format = file_format
        self.header = header
        self.inferschema = inferschema
        self.path = path

    def load_file(self):
        # global spark
        df = None #Initialize df to None
        try:
            df = self.spark.read.format(self.format) \
            .option("header", self.header) \
            .option("inferSchema", self.inferschema) \
            .load(self.path)

            print(f"File has {df.count()} rows")

            print("Showing the first ten rows in your file")
            df.show(10)

        except Exception as e:
            print(f"Your data could not be loaded. Error: {str(e)}")
            return "Failed to load the file"

def main():
    app = Dataloader("csv","true","true","../../../../data/housing.csv")
    app.load_file()

if __name__ == "__main__":
    main()