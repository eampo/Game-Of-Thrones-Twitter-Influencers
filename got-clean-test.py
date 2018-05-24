import apache_beam as beam
import csv
import pandas as pd
import re

def clean_text(text):
    tokens = re.split('\W+', text)
    text = [word for word in tokens if word in source]
    for text in text:
        str(text)
    return text


if __name__ == '__main__':
   with beam.Pipeline('DirectRunner') as pipeline:

      got = (pipeline
         | 'got:read' >> beam.io.ReadFromText('got_tweets.csv')
         | 'got:fields' >> beam.Map(lambda line: next(csv.reader([line])))
         | 'got:tz' >> beam.Map(lambda fields: (fields[1], clean_text(fields[4])))
      )

      got | beam.io.textio.WriteToText('cleaned_source')

      pipeline.run()