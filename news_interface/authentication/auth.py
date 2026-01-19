import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

class Authenticator:
    def __init__(self, config_path='config.yaml'):
        with open(config_path) as file:
            self.config = yaml.load(file, Loader=SafeLoader)
        self.authenticator = stauth.Authenticate(
            self.config['credentials'],
            self.config['cookie']['name'],
            self.config['cookie']['key'],
            self.config['cookie']['expiry_days']
        )

    def login(self):
        # Renderiza el formulario en sidebar
        self.authenticator.login(location='sidebar')

    def logout(self):
        self.authenticator.logout(location='sidebar')
        
        # limpiar estados
        keys_to_clear = ['admin_logged_in', 'show_admin_login', 
                         'authentication_status', 'username', 
                         'name', 'logout', 'roles', 'init']
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]

    def get_user_role(self):
        username = st.session_state.get('username')
        if username:
            roles = self.config['credentials']['usernames'][username].get('roles', [])
            return roles[0] if roles else None
        return None