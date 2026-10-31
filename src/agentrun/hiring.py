"""Offline HN hiring demo using a deterministic mock, never a model API.

Input is {"thread_id": int, "comments": [{"id": int, "text": str, ...}]}.
The mock recognizes headers in the form Company | Role | Location | REMOTE.
It leaves unknown fields null and skips comments without a hiring header.
"""

from html.parser import HTMLParser
import json
import re
from urllib.parse import urlsplit

from .mock import save_response
from .runner import Job, Step, StepContext


class CommentHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag in ("p", "br", "div", "li"):
            self.parts.append("\n")
        if tag == "a":
            href = dict(attrs).get("href")
            if not href:
                return
            try:
                scheme = urlsplit(href).scheme.lower()
            except ValueError:
                return
            if scheme in ("http", "https", "mailto"):
                self.links.append(href)

    def handle_endtag(self, tag):
        if tag in ("p", "div", "li"):
            self.parts.append("\n")

    def handle_data(self, data):
        self.parts.append(data)


def positive_id(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def prepare_comments(context: StepContext, value) -> dict:
    if not isinstance(value, dict) or not positive_id(value.get("thread_id")):
        raise ValueError("hn_hiring needs a positive integer thread_id")
    if not isinstance(value.get("comments"), list):
        raise ValueError("comments must be a list of HN comment objects")
    comments = {}
    for comment in value["comments"]:
        if not isinstance(comment, dict):
            raise ValueError("each comment must be an object")
        if comment.get("deleted") or comment.get("dead"):
            continue
        if not positive_id(comment.get("id")) or not isinstance(comment.get("text"), str):
            raise ValueError("each live comment needs a positive integer id and string text")
        # Supplied replies are not job postings. Missing parent means top level.
        if comment.get("parent", value["thread_id"]) != value["thread_id"]:
            continue
        parser = CommentHTML()
        parser.feed(comment["text"])
        parser.close()
        text = "\n".join(line.strip() for line in "".join(parser.parts).splitlines() if line.strip())
        if not text:
            continue
        normalized = {"id": comment["id"], "text": text, "links": parser.links}
        previous = comments.get(comment["id"])
        if previous is not None and previous != normalized:
            raise ValueError(f"conflicting copies of comment {comment['id']}")
        comments[comment["id"]] = normalized
    return {"thread_id": value["thread_id"], "comments": list(comments.values())}


def mock_extract(context: StepContext, value: dict) -> dict:
    listings = []
    for comment in value["comments"]:
        header = comment["text"].splitlines()[0]
        parts = [part.strip() for part in header.split("|")]
        if len(parts) < 3 or not all(parts[:3]):
            continue
        if re.search(r"\b(?:no|not)\s+remote\b", header, re.I):
            remote = False
        elif re.search(r"\bremote\b", header, re.I):
            remote = True
        elif re.search(r"\bonsite\b|\bon-site\b", header, re.I):
            remote = False
        else:
            remote = None
        location = None if re.fullmatch(r"remote", parts[2], re.I) else parts[2]
        listings.append({
            "comment_id": comment["id"],
            "source_url": f"https://news.ycombinator.com/item?id={comment['id']}",
            "company": parts[0],
            "role": parts[1],
            "location": location,
            "remote": remote,
            "apply_url": next(iter(comment["links"]), None),
            "text": comment["text"],
        })
    result = {"thread_id": value["thread_id"], "listings": listings}
    if not value["comments"]:
        return result
    receipt = save_response(context, result)
    # Simulated usage only, matching the example's local provider ledger.
    context.report_usage(
        input_tokens=len(json.dumps(value).split()),
        output_tokens=len(json.dumps(receipt["result"]).split()),
        cost_usd="0.10",
    )
    return receipt["result"]


HN_HIRING = Job((
    Step("prepare_comments", prepare_comments, max_attempts=1),
    Step("extract_hiring", mock_extract),
))
