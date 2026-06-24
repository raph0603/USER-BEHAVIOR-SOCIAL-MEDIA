import streamlit as st


def render_navigation():
    with st.container(
        horizontal=True,
        vertical_alignment="center",
        gap="small",
    ):
        st.link_button(
            label="Dashboard",
            url="/",
            icon=":material/dashboard:",
            type="tertiary",
        )
        st.link_button(
            label="Configuration",
            url="/Configuration",
            icon=":material/settings:",
            type="tertiary",
        )
    st.divider()
