#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#

from rdr_service.data_gen.generators.participant import ParticipantGen
from rdr_service.data_gen.generators.hpo import HPOGen

class SpecParticipantMixin:
    """
    Unittest structure for testing spec generated participant data.  Uses the generators
    found in rdr_service/data_gen/generators.

    Warning: Since this code uses the APIs, its not possible to use FakeClock.
    """

    _spec_generators = dict()  # cache generator objects.

    def get_spec_generator(self, gen_id):
        """
        Retrieve a spec data generator object.
        :param gen_id: Generator name
        :return: Generator object or raise ValueError exception.
        """
        if gen_id == 'ParticipantGen':
            if 'ParticipantGen' not in self._spec_generators:
                self._spec_generators['ParticipantGen'] = ParticipantGen()
            return self._spec_generators['ParticipantGen']

        if gen_id == 'HPOGen':
            if 'HPOGen' not in self._spec_generators:
                self._spec_generators['HPOGen'] = HPOGen()
            return self._spec_generators['HPOGen']

        # TODO: Add the other spec generators here

        raise ValueError('Invalid Spec Generator ID argument.')


    def spec_participant(self, spec_data=None):
        """
        :param spec_data: dict containing specific values for test participant.
        """
        if not isinstance(spec_data, dict):
            spec_data = dict()

        # Get our spec generator objects
        hpo_id = spec_data.get('_HPO', None)
        site_id = spec_data.get('_HPOSite', None)

        #
        # Create Participant
        #
        p_gen = self.get_spec_generator('ParticipantGen')
        hpo_gen = self.get_spec_generator('HPOGen')
        hpo_site = None

        if site_id:
            # if site_id is given, it also returns the HPO the site is matched with.
            hpo_site = hpo_gen.get_site(site_id)
        if hpo_id and not hpo_site:
            # if hpo is given, select a random site within the hpo.
            hpo_site = hpo_gen.get_hpo(hpo_id).get_random_site()
        if not hpo_site:
            # choose a random hpo and site.
            hpo_site = hpo_gen.get_random_site()

        # make a new spec test participant.
        p_obj = p_gen.new(hpo_site)

        response = self.send_post("Participant", p_obj.to_dict())

        # Merge the response data with the participant object
        p_obj.update(response)

        # Update participant if have assigned a specific hpo site to this participant.
        if hpo_site.id:
            headers = {'If-Match': response['meta']['versionId']}
            response['organization'] = hpo_site.org_id
            response['site'] = hpo_site.id

            response = self.send_put(f'Participant/{p_obj.participantId}', response, headers=headers)

            p_obj.update(response)

        return p_obj

        #
        # Questionnaire Modules
        #
        # TODO: Add code to codebook generator to write the codebook to a temporary file and load it on init,
        #       we don't want to keep downloading the codebook from the web for each unit test. So if we
        #       download it to a named tempfile and then check for it and load it each time the codebook object
        #       is created, it will save a lot of time.
        #  See: rdr_service.data_gen.generators.codebook.py.

        # TODO: Create function to submit questionnaire modules specified in spec_data if '_PPIModule' found
        #       in spec_data.
        #  See: rdr_service.client.client_libs.spec_data_generator.py.

        #
        # Physical Measurements
        #
        # TODO: Create function to submit physical measurements if '_PM' key is 'yes' in spec_data.
        #  See: rdr_service.client.client_libs.spec_data_generator.py.

        #
        # Biobank Orders
        #
        # TODO: Create function submit biobank orders from spec_data if '_BIOOrder' key is found in spec_data.
        #  See: rdr_service.client.client_libs.spec_data_generator.py.

        #
        # BioBank Stored Samples
        #
        # TODO: Future: Create function and code to create biobank stored sample records from order data.
        #       Possible key name in spec_data: '_BIOStoredSample'

        # TODO: Future: Implement function and code to create genomic records.


