import os
from functools import reduce
from typing import Dict, List

import flask
from scrapinghub import DuplicateJobError, ScrapinghubClient

application = flask.Flask(__name__)
hub = ScrapinghubClient(os.environ["SCRAPINGHUB_APIKEY"])
project = hub.get_project(int(os.environ["SCRAPINGHUB_PROJECT"]))
spider = project.spiders.get("juris")


def group_by_state(value: Dict[str, List], job):
    state = job["state"]
    return {**value, state: [*value.get(state, []), job]}


@application.route("/", methods=["GET"])
def index():
    return flask.render_template("index.html")


@application.route("/jobs/", methods=["GET"])
def list_jobs():
    # print([*spider.jobs.iter()])
    return {group["name"]: group["summary"] for group in spider.jobs.summary()}
    # return reduce(group_by_state, spider.jobs.iter(), {})


@application.route("/jobs/", methods=["POST"])
def create_job():
    url = flask.request.form["url"]
    try:
        job = spider.jobs.run(job_args={"url": url}, meta={"url": url})
        return {"key": job.key}, 201
    except DuplicateJobError:
        return "", 425


if __name__ == "__main__":
    application.run()
