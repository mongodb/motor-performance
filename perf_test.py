# Copyright 2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the MongoDB Driver Performance Benchmarking Spec. Python 3.5+."""

import json
import os
import tarfile
import tempfile
import sys
from time import time
from os.path import join, realpath, exists, dirname, isdir

from gridfs import GridFS
from tornado import gen

from motor import motor_tornado
from test import tornado_tests, unittest
from test.test_environment import env

from urllib.parse import urljoin
from urllib.request import urlretrieve


TEST_PATH = join(
    dirname(realpath(__file__)),
    join('performance_testdata'))

BASE = ("https://github.com/ajdavis/driver-performance-test-data/"
        "raw/add-closing-brace/")

NUM_ITERATIONS = 100
MAX_ITERATION_TIME = 300
NUM_DOCS = 10000
fast_perf_tests = False

# Shortcut for testing.
if os.environ.get('FAST_PERF_TESTS'):
    NUM_ITERATIONS = 1
    MAX_ITERATION_TIME = 30
    NUM_DOCS = 10
    fast_perf_tests = True


def download_test_data():
    if not isdir(TEST_PATH):
        raise Exception("No directory '%s'" % (TEST_PATH, ))

    for name in "single_and_multi_document", "parallel":
        target_dir = join(TEST_PATH, name)
        if not exists(target_dir):
            print('Downloading %s.tgz' % (name, ))
            file_path = join(TEST_PATH, name + ".tgz")
            urlretrieve(urljoin(BASE, name + ".tgz"), file_path)

            # Each tgz contains a single directory, e.g. "parallel.tgz" expands
            # to a directory called "parallel" full of data files.
            with tarfile.open(file_path, "r:gz") as tar:
                def is_within_directory(directory, target):
                    
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    
                    return prefix == abs_directory
                
                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                
                    for member in tar.getmembers():
                        member_path = os.path.join(path, member.name)
                        if not is_within_directory(path, member_path):
                            raise Exception("Attempted Path Traversal in Tar File")
                
                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                    
                
                safe_extract(tar, path=TEST_PATH)
                tar.close()


class Timer(object):
    def __enter__(self):
        self.start = time()
        return self

    def __exit__(self, *args):
        self.end = time()
        self.interval = self.end - self.start


class _PerformanceTest(tornado_tests.MotorTest):
    def setUp(self):
        super().setUp()
        download_test_data()
        env.sync_cx.drop_database('perftest')

        # In case we're killed mid-test, print its name before starting.
        sys.stdout.write("{:<30}".format(self.__class__.__name__))
        sys.stdout.flush()

    def before(self):
        pass

    async def do_task(self):
        raise NotImplementedError

    def percentile(self, percentile):
        if hasattr(self, 'results'):
            sorted_results = sorted(self.results)
            percentile_index = int(len(sorted_results) * percentile / 100) - 1
            return sorted_results[percentile_index]
        else:
            self.fail('Test execution failed')

    def runTest(self):
        results = []
        start = time()
        self.max_iterations = NUM_ITERATIONS
        for i in range(NUM_ITERATIONS):
            if time() - start > MAX_ITERATION_TIME:
                break
            self.before()
            with Timer() as timer:
                self.io_loop.run_sync(self.do_task)
            results.append(timer.interval)

        self.results = results

    def tearDown(self):
        # Finish writing test line.
        sys.stdout.write("{:>5.3f}\n".format(self.percentile(50)))
        env.sync_cx.drop_database('perftest')
        super().tearDown()


# SINGLE-DOC BENCHMARKS
class TestRunCommand(_PerformanceTest):
    async def do_task(self):
        isMaster = {'isMaster': True}
        admin = self.cx.admin
        for _ in range(NUM_DOCS):
            await admin.command(isMaster)


def load_doc(dataset):
    path = join(TEST_PATH, 'single_and_multi_document', dataset)
    with open(path, 'r') as data:
        return json.loads(data.read())


class TestFindOneByID(_PerformanceTest):
    def setUp(self):
        super().setUp()
        doc = load_doc('tweet.json')
        documents = [doc.copy() for _ in range(NUM_DOCS)]
        self.inserted_ids = env.sync_cx.perftest.corpus.insert(documents)

    async def do_task(self):
        corpus = self.cx.perftest.corpus
        for i in self.inserted_ids:
            await corpus.find_one({'_id': i})


class TestSmallDocInsertOne(_PerformanceTest):
    def before(self):
        env.sync_cx.perftest.drop_collection('corpus')
        env.sync_cx.perftest.command({'create': 'corpus'})

        # Recreate documents, since the previous run added _id to each.
        doc = load_doc('small_doc.json')
        self.documents = [doc.copy() for _ in range(NUM_DOCS)]

    async def do_task(self):
        corpus = self.cx.perftest.corpus
        for doc in self.documents:
            await corpus.insert(doc)


class TestLargeDocInsertOne(_PerformanceTest):
    def before(self):
        env.sync_cx.perftest.drop_collection('corpus')
        env.sync_cx.perftest.command({'create': 'corpus'})
        doc = load_doc('large_doc.json')
        self.documents = [doc.copy() for _ in range(10)]

    async def do_task(self):
        corpus = self.cx.perftest.corpus
        for doc in self.documents:
            await corpus.insert(doc)


# MULTI-DOC BENCHMARKS
class TestFindManyAndEmptyCursor(_PerformanceTest):
    def setUp(self):
        super().setUp()
        doc = load_doc('tweet.json')
        for _ in range(10):
            env.sync_cx.perftest.command('insert', 'corpus',
                                         documents=[doc] * 1000)

    async def do_task(self):
        corpus = self.cx.perftest.corpus
        for _ in await corpus.find().to_list(length=None):
            pass


class TestSmallDocBulkInsert(_PerformanceTest):
    def before(self):
        env.sync_cx.perftest.drop_collection('corpus')
        env.sync_cx.perftest.command({'create': 'corpus'})
        doc = load_doc('small_doc.json')
        self.documents = [doc.copy() for _ in range(NUM_DOCS)]

    async def do_task(self):
        corpus = self.cx.perftest.corpus
        await corpus.insert(self.documents)


class TestLargeDocBulkInsert(_PerformanceTest):
    def setUp(self):
        super().setUp()
        doc = load_doc('large_doc.json')
        self.documents = [doc.copy() for _ in range(10)]

    def before(self):
        env.sync_cx.perftest.drop_collection('corpus')
        env.sync_cx.perftest.command({'create': 'corpus'})

    async def do_task(self):
        corpus = self.cx.perftest.corpus
        await corpus.insert(self.documents)


gridfs_path = join(TEST_PATH, 'single_and_multi_document', 'gridfs_large.bin')


class TestGridFsUpload(_PerformanceTest):
    def setUp(self):
        super().setUp()

        with open(gridfs_path, 'rb') as data:
            self.document = data.read()

    def before(self):
        env.sync_cx.perftest.drop_collection('fs.files')
        env.sync_cx.perftest.drop_collection('fs.chunks')
        GridFS(env.sync_cx.perftest).put(b'x', filename='init')

        # Need new client that doesn't have dropped GridFS indexes in its cache.
        db = self.motor_client(ssl=self.ssl).perftest
        self.gridfs = motor_tornado.MotorGridFS(db)

    async def do_task(self):
        await self.gridfs.put(self.document, filename='gridfstest')


class TestGridFsDownload(_PerformanceTest):
    def setUp(self):
        super().setUp()
        self.gridfs = motor_tornado.MotorGridFS(self.cx.perftest)

        with open(gridfs_path, 'rb') as data:
            self.uploaded_id = GridFS(env.sync_cx.perftest).put(data)

    async def do_task(self):
        out = await self.gridfs.get(self.uploaded_id)
        await out.read()


# PARALLEL BENCHMARKS
async def insert_json_file(collection, filename):
    documents = []
    with open(filename, 'r') as data:
        for line in data:
            documents.append(json.loads(line.strip()))

    await collection.insert(documents)


async def insert_json_file_with_file_id(collection, filename):
    documents = []
    with open(filename, 'r') as data:
        for line in data:
            doc = json.loads(line)
            doc['file'] = filename
            documents.append(doc)

    collection.insert(documents)


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


async def insert_json_files(collection, files):
    # A few files at a time to avoid OOM.
    for chunk in chunks(files, 20):
        await gen.multi(insert_json_file(collection, f) for f in chunk)


async def read_json_file(collection, filename):
    files = await collection.find({'file': filename}).to_list(length=None)
    with tempfile.TemporaryFile() as tmp:
        for doc in files:
            tmp.write(str(doc) + '\n')


async def insert_gridfs_file(motor_gridfs, filename):
    with open(filename, 'rb') as gfile:
        await motor_gridfs.put(gfile, filename=filename)


async def read_gridfs_file(motor_gridfs, filename):
    with tempfile.TemporaryFile() as tmp:
        out = await motor_gridfs.get_last_version(filename)
        tmp.write(await out.read())


class TestJsonMultiImport(_PerformanceTest):
    def setUp(self):
        super().setUp()
        ldjson_path = join(TEST_PATH, 'parallel', 'ldjson_multi')
        self.files = [join(ldjson_path, s) for s in os.listdir(ldjson_path)]
        if fast_perf_tests:
            self.files = self.files[:10]

        self.corpus = self.cx.perftest.corpus

    def before(self):
        env.sync_cx.perftest.drop_collection('corpus')
        env.sync_cx.perftest.command({'create': 'corpus'})

    async def do_task(self):
        await insert_json_files(self.corpus, self.files)


class TestJsonMultiExport(_PerformanceTest):
    def setUp(self):
        super().setUp()
        env.sync_cx.perfest.corpus.create_index('file')

        ldjson_path = join(TEST_PATH, 'parallel', 'ldjson_multi')
        self.files = [join(ldjson_path, s) for s in os.listdir(ldjson_path)]
        if fast_perf_tests:
            self.files = self.files[:10]

        self.corpus = self.cx.perftest.corpus
        self.io_loop.run_sync(lambda: insert_json_files(self.corpus,
                                                        self.files))

    async def do_task(self):
        # A few files at a time to avoid OOM.
        for chunk in chunks(self.files, 20):
            await gen.multi(read_json_file(self.corpus, f) for f in chunk)


class TestGridFsMultiFileUpload(_PerformanceTest):
    def setUp(self):
        super().setUp()
        path = join(TEST_PATH, 'parallel', 'gridfs_multi')
        self.files = [join(path, s) for s in os.listdir(path)]

    def before(self):
        env.sync_cx.perftest.drop_collection('fs.files')
        env.sync_cx.perftest.drop_collection('fs.chunks')

        # Need new client that doesn't have GridFS indexes in its index cache.
        db = self.motor_client(ssl=self.ssl).perftest
        self.gridfs = motor_tornado.MotorGridFS(db)

    async def do_task(self):
        await gen.multi(insert_gridfs_file(self.gridfs, f) for f in self.files)


class TestGridFsMultiFileDownload(_PerformanceTest):
    def setUp(self):
        super().setUp()

        sync_gridfs = GridFS(env.sync_cx.perftest)
        path = join(TEST_PATH, 'parallel', 'gridfs_multi')
        self.files = [join(path, s) for s in os.listdir(path)]

        for fname in self.files:
            with open(fname, 'rb') as gfile:
                sync_gridfs.put(gfile, filename=fname)

        self.gridfs = motor_tornado.MotorGridFS(self.cx.perftest)

    async def do_task(self):
        await gen.multi(read_gridfs_file(self.gridfs, f) for f in self.files)


del _PerformanceTest  # Don't run this base class as a test.


if __name__ == "__main__":
    env.setup()
    unittest.main(verbosity=0)  # Suppress dots in output.
