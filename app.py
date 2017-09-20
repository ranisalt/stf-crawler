import flask
from multiprocessing import Process, Queue
from os import path
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from stf.spiders.juris import JurisSpider


class CrawlerWorker(Process):
    def __init__(self, flt):
        Process.__init__(self)
        self.flt = flt
        self.process = CrawlerProcess(get_project_settings())

    def run(self):
        self.process.crawl(JurisSpider, flt=self.flt)
        self.process.start()
        with open(path.join(path.dirname(path.abspath(__file__)), 'static', '{}.txt'.format(self.flt)), 'w') as fp:
            fp.writelines(self.process.spider_loader._spiders['juris'].output)


def crawl(flt):
    worker = CrawlerWorker(flt)
    worker.start()


app = flask.Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    return flask.render_template('index.html')


@app.route('/flt/', methods=['GET'])
def get_doutrinas():
    flt = flask.request.args.get('filter')
    crawl(flt)
    return flask.redirect('/')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', dest='debug_mode',
                        default=False)
    args = parser.parse_args()
    app_options = {
            'debug': args.debug_mode,
            'use_debugger': not args.debug_mode,
            'use_reloader': not args.debug_mode,
        } if args.debug_mode else {}

    app.run(**app_options)
