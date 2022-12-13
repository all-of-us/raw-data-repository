from graphene.validation import depth_limit_validator
from graphql import validate, parse, GraphQLError, GraphQLSyntaxError
from flask import request

# from rdr_service.api_util import RTI_AND_HEALTHPRO
# from rdr_service import app_util
from rdr_service.api.nph_participant_api_schemas.schema import NPHParticipantSchema
from rdr_service.api.nph_participant_api_schemas.util import validation_error_message, error_message


# @app_util.auth_required(RTI_AND_HEALTHPRO)
def nph_participant():

    try:
        query = request.get_data().decode('utf8')
        validate_error = validate_query(query)
        if validate_error:
            return validation_error_message(validate_error), 400
        result = NPHParticipantSchema.execute(query)
        if result.errors:
            return validation_error_message(result.errors), 400
        return result.data, 200
    except GraphQLSyntaxError as syntax_error:
        return error_message(syntax_error.formatted), 400
    except GraphQLError as graphql_error:
        return error_message(graphql_error.formatted), 400


def validate_query(request_query_string):
    try:
        parsed_query = parse(source=request_query_string, no_location=True)

        validation_errors = validate(
            schema=NPHParticipantSchema.graphql_schema,
            document_ast=parsed_query,
            rules=(
                depth_limit_validator(max_depth=10),
            )
        )

        return validation_errors
    except GraphQLSyntaxError:
        raise
