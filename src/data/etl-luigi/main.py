import luigi
from extractRawFiles import ExtractRawFiles

if __name__ == '__main__':
    luigi.build([ExtractRawFiles()], local_scheduler=False)
