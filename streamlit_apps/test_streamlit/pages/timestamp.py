import streamlit as st
from datetime import datetime

def show_timestamp_page():
    st.title("Current Timestamp")
    
    # Create a container for the timestamp
    timestamp_container = st.empty()
    
    # Function to update timestamp
    def update_timestamp():
        current_time = datetime.now()
        timestamp_container.metric(
            "Current Time",
            current_time.strftime("%Y-%m-%d %H:%M:%S"),
            delta=None
        )
    
    # Initial timestamp
    update_timestamp()
    
    # Add a refresh button
    if st.button("Refresh Timestamp"):
        update_timestamp()
    
    # Add some additional time information
    st.subheader("Time Details")
    current_time = datetime.now()
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Hour", current_time.hour)
    with col2:
        st.metric("Minute", current_time.minute)
    with col3:
        st.metric("Second", current_time.second)
    
    # Add timezone information
    st.subheader("Timezone Information")
    st.write(f"Timezone: {datetime.now().astimezone().tzinfo}")
    st.write(f"UTC Offset: {datetime.now().astimezone().utcoffset()}") 