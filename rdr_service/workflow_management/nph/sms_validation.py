from rdr_service.services.ancillary_studies.nph_incident import create_nph_incident


class SmsValidator:
    def __init__(self, file_path, recipient):
        self.file_path = file_path
        self.recipient = recipient

    def validate_bmi_value(
        self, row: dict[str, str], max_length_allowed: int = 5
    ) -> None:
        """If bmi str length is greater than 5 chars (including the decimal), raise value error and send slack alert."""
        if row.get("bmi"):
            bmi_str_len = len(row.get("bmi"))
            if bmi_str_len > max_length_allowed:
                msg = (
                    f"Pull list Validation Error: BMI value exceeds the max length allowed of {max_length_allowed} "
                    f"and contains {bmi_str_len} characters for sample id {row.get('sample_id')}."
                    f"Check the data at: {self.file_path}."
                )
                create_nph_incident(slack=True, message=msg)
                raise ValueError(
                    f"BMI str length of {bmi_str_len} exceeds the limit of {max_length_allowed} characters."
                )

    def validate_pull_list(self, data: list[dict]) -> None:
        """Run validation checks for each row in the pull list."""
        for row in data:
            self.validate_bmi_value(row)

    def validate_values_exist_if_biobank_id_exists(self, row) -> None:
        """
        For N1 generation, check if biobank id exists in a row, then all other values defined in required_columns
        exist. If not, raise an exception and send a slack alert.

        N1 manifest contains information related to each individual's sample within each compartment (well) of a plate.
        So if a biobank_id is present in a row in N1 manifest, indicating that an individual’s sample must be in a well
        box position, then it must also contain a sample_id and other details of that individual. If it is missing those
        information, then we need to investigate why the values for that biobank_id is not populated.

        :param row: A row representing data.
        :type row: class: 'sqlalchemy.util._collections.result'
        :raise ValueError: If other values does not exist when biobank_id exists.
        """
        required_columns = [
            row.sample_id,
            row.sample_identifier,
            row.sex_at_birth,
            row.destination,
            row.ethnicity,
            row.race,
            row.bmi,
            row.diet,
        ]
        if row.biobank_id:
            if not all([col for col in required_columns]):
                msg = (
                    f"N1 manifest for package id {row.package_id} contains biobank id {row.biobank_id} "
                    f"but is missing other value(s) for recipient {self.recipient}."
                )
                create_nph_incident(slack=True, message=msg)
                raise ValueError(
                    "Biobank ID exists in N1, but other required values are missing."
                )

    def validate_n1(self, data: list) -> None:
        """
        Run validation checks for each row in N1.

        :param data: List of SQLAlchemy result objects obtained from query.all() when tables are joined to create N1.
        :type data: list[sqlalchemy.util._collections.result]
        """
        for row in data:
            self.validate_values_exist_if_biobank_id_exists(row)
