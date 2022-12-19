
from rdr_service.model.profile_update import ProfileUpdate
from rdr_service.repository import BaseRepository


class ProfileUpdateRepository(BaseRepository):
    def store_update_json(self, json):
        self._add_to_session(ProfileUpdate(json=json))
