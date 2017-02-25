FROM ray-project/deploy:latest
ADD benchmark.tar /benchmark
ADD git-rev /benchmark/git-rev
WORKDIR /benchmark
RUN python setup.py install
