# Messenger Data Analysis
Analyse Messenger data JSON exports from Facebook.

## Usage
Export Facebook messages data and store in `../data/` so the directory `../data/messages/inbox` contains directories for every conversation.

##### Initialise analyser for a particular chat
```python
chat = MessengerData('GroupChatName_eoK-g8wEea')
```

##### Dataframe of messages
```python
chat.messages_df()
```
Columns: ['sender_name', 'type', 'content', 'call_duration', 'time']

##### Message count by sender
```python
chat.get_no_of_messages_by_sender()
```