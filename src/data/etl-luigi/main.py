import luigi
from extractRawFiles import ExtractRawFiles
from multiprocessing import Manager, Pool


def build(city):
    luigi.build([ExtractRawFiles(city=city)], local_scheduler=False)


if __name__ == '__main__':
    pool = Pool(processes=6)
    list = ['Paris',
            'London',
            'Madrid',
            'Barcelona',
            'Berlin',
            'Rome',
            'Athens',
            'Milan',
            'Stuttgart',
            'Frankfurt',
            'Lisbon',
            'Katowice',
            'Vienna',
            'Mannheim',
            'Birmingham',
            'Naples',
            'Manchester',
            'Bucharest',
            'Hamburg',
            'Brussels',
            'Essen',
            'Warsaw',
            'Budapest',
            'Turin',
            'Leeds',
            'Florence',
            'Lyon',
            'The+Hague',
            'Marseille',
            'Porto',
            'Sheffield',
            'Duisburg',
            'Munich',
            'Stockholm',
            'Dusseldorf',
            'Seville',
            'Sofia',
            'Prague',
            'Glasgow',
            'Helsinki',
            'Kobenhavn',
            'Dublin',
            'Lille',
            'Amsterdam',
            'Rotterdam',
            'Cologne']
    pool.map(build, list)
