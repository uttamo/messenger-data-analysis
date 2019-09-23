import os
import os.path
import json
import logging

import pandas as pd
from ftfy import fix_text

logging.getLogger().setLevel(logging.DEBUG)

DEFAULT_INBOX_PATH = os.path.join('data', 'messages', 'inbox')


class MessengerData:
    def __init__(self, chat_path: str):
        self.chat_id = os.path.basename(chat_path)
        messages_file = self._load_messages_file(chat_path)
        self.df = self._load_messages_df(messages_file)
        self.chat_type = messages_file['thread_type']  # 'RegularGroup', 'Regular'
        self.chat_title = messages_file['title']

    def _load_messages_file(self, chat_path: str) -> dict:
        logging.info(f"Loading messages file '{chat_path}'")
        assert os.path.exists(chat_path)
        json_files = [fn for fn in os.listdir(chat_path) if fn.endswith('.json')]
        assert len(json_files) == 1, 'There should be 1 JSON file'
        json_file = json_files[0]
        assert json_file == 'message_1.json'
        messages_file_path = os.path.join(chat_path, json_file)
        with open(messages_file_path, 'r') as inp:
            json_file = json.load(inp)
        return json_file

    def _load_messages_df(self, json_file: dict) -> pd.DataFrame:
        logging.info(f"Loading messages dataframe")
        messages_raw_data = json_file['messages']
        df = pd.DataFrame(messages_raw_data)
        # Convert UNIX timestamps (ms) to proper datetimes
        df['timestamp_s'] = df['timestamp_ms'] / 1000
        df['time'] = pd.to_datetime(df['timestamp_s'], unit='s')

        # Sort in chronological order
        df = df.sort_values('time', ascending=True).reset_index().drop('index', axis=1)

        # Only interested in 'normal' messages
        # df = df[df['type'] == 'Generic']

        # Fix incorrect text decoding
        df['content'] = df['content'].fillna('').apply(fix_text)

        final_columns = [c for c in ['sender_name', 'type', 'content', 'call_duration', 'time'] if c in df.columns]

        df = df[final_columns]
        return df

    def __repr__(self):
        return "<MessengerData for {} (rows={})>".format(self.chat_id, len(self))

    def __len__(self):
        return len(self.df)

    def __gt__(self, other):
        return len(self) > len(other)

    def __ge__(self, other):
        return len(self) >= len(other)

    def _get_specific_msg_type_df(self, message_type: str) -> pd.DataFrame:
        return self.df[self.df['type'] == message_type]

    def calls_df(self):
        return self._get_specific_msg_type_df('Call')

    def messages_df(self):
        return self._get_specific_msg_type_df('Generic')

    def get_no_of_messages_by_sender(self):
        msgs_by_sender = self.df.groupby('sender_name').size().to_frame('msg_count')
        msgs_by_sender['msg_count_pct'] = 100 * msgs_by_sender['msg_count'] / msgs_by_sender['msg_count'].sum()
        return msgs_by_sender.sort_values('msg_count', ascending=False)

    def _get_message_count_by_period(self, period: str):
        return self.df.groupby(pd.Grouper(key='time', freq=period)).size()

    def get_message_count_by_year(self):
        return self._get_message_count_by_period('Y')

    def get_message_count_by_month(self):
        return self._get_message_count_by_period('M')

jack = MessengerData(os.path.join('data', 'messages', 'inbox', 'JackWmpy_wK_55Entpg'))
import pdb; pdb.set_trace()