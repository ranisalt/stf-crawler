import os
from functools import reduce
from typing import Dict, List

import flask
from scrapinghub import DuplicateJobError, ScrapinghubClient

# class CrawlerWorker(Process):
#     def __init__(self, flt):
#         Process.__init__(self)
#         self.flt = flt
#         self.process = CrawlerProcess(get_project_settings())

#     def run(self):
#         self.process.crawl(JurisSpider, flt=self.flt)
#         self.process.start()
#         with open(
#             path.join(
#                 path.dirname(path.abspath(__file__)),
#                 "static",
#                 "{}.txt".format(self.flt),
#             ),
#             "w",
#         ) as fp:
#             fp.writelines(self.process.spider_loader._spiders["juris"].output)


# def crawl(flt):
#     worker = CrawlerWorker(flt)
#     worker.start()


app = flask.Flask(__name__)
hub = ScrapinghubClient(os.environ["SCRAPINGHUB_APIKEY"])
project = hub.get_project(int(os.environ["SCRAPINGHUB_PROJECT"]))
spider = project.spiders.get("juris")


def group_by_state(value: Dict[str, List], job):
    state = job["state"]
    return {**value, state: [*value.get(state, []), job]}


@app.route("/", methods=["GET"])
def index():
    return flask.render_template("index.html")


@app.route("/jobs/", methods=["GET"])
def list_jobs():
    # print([*spider.jobs.iter()])
    return {group["name"]: group["summary"] for group in spider.jobs.summary()}
    # return reduce(group_by_state, spider.jobs.iter(), {})


@app.route("/jobs/", methods=["POST"])
def create_job():
    url = flask.request.form["url"]
    try:
        job = spider.jobs.run(job_args={"url": url}, meta={"url": url})
        return {"key": job.key}, 201
    except DuplicateJobError:
        return "", 425


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--debug", action="store_true", dest="debug_mode", default=False
    )
    args = parser.parse_args()
    app_options = (
        {
            "debug": args.debug_mode,
            "use_debugger": not args.debug_mode,
            "use_reloader": not args.debug_mode,
        }
        if args.debug_mode
        else {}
    )

    app.run(**app_options)
