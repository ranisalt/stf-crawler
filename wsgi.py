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
    return {group["name"]: group["summary"] for group in spider.jobs.summary()}


@application.route("/jobs/", methods=["POST"])
def create_job():
    url = flask.request.form["url"]
    try:
        job = spider.jobs.run(job_args={"url": url}, meta={"url": url})
        return {"key": job.key}, 201
    except DuplicateJobError:
        return "", 425


@application.route("/jobs/<int:job_id>.<ext>", methods=["GET"])
def show_job(job_id: int, ext):
    key = f"{spider.key}/{job_id}"
    print(key)
    job = spider.jobs.get(key)
    lines = (i["lines"] for i in job.items.iter())
    if ext == "json":
        return {"items": [*lines]}
    if ext == "txt":
        return "\n\n".join(lines), {"Content-Type": "text/plain; charset=utf-8"}


if __name__ == "__main__":
    application.run()
