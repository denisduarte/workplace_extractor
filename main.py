from workplace_extractor import Extractor

def run():
  token = '/Users/denisduarte/Petrobras/PythonProjects/workplace_extractor/access.token'
  config = '/Users/denisduarte/Petrobras/PythonProjects/workplace_extractor/config.ini'
  export = 'PEOPLE'
  hashtags = ''
  since = '2021-05-01'
  until = '2021-06-01'
  csv = 'csv.csv'
  loglevel = 'NONE'

  wp_extractor = Extractor(token, config, export, hashtags, since, until, csv, loglevel)
  return wp_extractor.extract()

print(run())
