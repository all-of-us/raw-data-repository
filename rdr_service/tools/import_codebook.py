"""Imports a codebook into the database, using JSON found at a specified path."""

import json
import logging

from rdr_service.dao.code_dao import CodeBookDao
from rdr_service.main_util import configure_logging, get_parser


def main(args):
    with open(args.file) as f:
        codebook_json = json.load(f)
        logging.info("Loaded codebook JSON from %r.", args.file)
        CodeBookDao().import_codebook(codebook_json)


if __name__ == "__main__":
    configure_logging()
    parser = get_parser()
    parser.add_argument("--file", help="Path to the JSON representation of the codebook.", required=True)

    main(parser.parse_args())
