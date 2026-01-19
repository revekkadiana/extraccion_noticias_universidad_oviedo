import streamlit as st
from news_interface.authentication.auth import Authenticator

def display_sidebar(authenticator):
    # Maneja el flujo de autenticación para acceder a admin
    if 'admin_logged_in' not in st.session_state:
        st.session_state['admin_logged_in'] = False
    if 'show_admin_login' not in st.session_state:
        st.session_state['show_admin_login'] = False

    st.sidebar.title("Panel")

    print(st.session_state)

    if not st.session_state['admin_logged_in']:
        st.sidebar.write("Interfaz pública - búsqueda de noticias")

        if not st.session_state['show_admin_login']:
            if st.sidebar.button("Acceso a Admin"):
                st.session_state['authentication_status'] = None
                st.session_state['show_admin_login'] = True
                st.session_state['admin_logged_in'] = False
                st.rerun()
        else:
            authenticator.login()

            if st.session_state.get("authentication_status") is True:
                role = authenticator.get_user_role()
                if role == 'admin':
                    st.session_state['admin_logged_in'] = True
                    st.sidebar.success(f"Bienvenido {st.session_state.get('name')} (Admin)")
                    st.rerun()
                else:
                    st.sidebar.error("No tienes permisos de administrador")
                    #authenticator.logout()
                    #st.session_state['authentication_status'] = None
                    #st.session_state['show_admin_login'] = False
                    authenticator.logout()
                    st.rerun()

            elif st.session_state.get("authentication_status") is False:
                st.sidebar.error("Usuario o contraseña incorrectos")
            else:
                st.sidebar.info("Por favor ingrese usuario y contraseña")

            if st.sidebar.button("Cancelar"):
                st.session_state['show_admin_login'] = False
                st.session_state['admin_logged_in'] = False
                st.session_state['authentication_status'] = None
                st.rerun()
    else:
        st.sidebar.success(f"Sesión de administrador activa: {st.session_state.get('name')}")
        if st.sidebar.button("Cerrar sesión de administrador"):
            #st.session_state['admin_logged_in'] = False
            #st.session_state['show_admin_login'] = False
            authenticator.logout()
            st.rerun()

