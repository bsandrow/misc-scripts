#!/usr/bin/env python

import re
import sys
import imaplib
import pprint
import sqlite3
from email.parser import HeaderParser

def split_folder_str(folder_str):
    match = re.search(r'\(([^)]+)\) "([^"]*)" "([^"]*)"', folder_str)
    if match is None:
        return ('','','')
    else:
        return (match.group(1), match.group(2), match.group(3))

def init_db(db):
    cursor = db.cursor()
    cursor.execute('''
    create table if not exists folders (
        folder_no integer primary key,
        name text
    )
    ''')
    cursor.execute('''
    create table if not exists messages (
        message_no integer primary key,
        message_id text
    )
    ''')
    cursor.execute('''
    create table if not exists fldr_msg_xref (
        message_no integer,
        folder_no integer
    )
    ''')
    cursor.execute(
        '''
        create table if not exists msg_meta (
            message_id integer,
            folder text,
            msgno integer
        )
        '''
    )
    db.commit()
    cursor.close()

def add_folder_to_db(db, folder):
    cursor = db.cursor()
    cursor.execute('select count(*) from folders where name = ?', (folder,))
    if cursor.fetchone()[0]:
        cursor.close()
        return
    cursor.execute('insert into folders (name) values (?)', (folder,))
    db.commit()
    cursor.close()

def add_msgid_to_db(db, msgid):
    cursor = db.cursor()
    cursor.execute('select count(*) from messages where message_id = ?', (msgid,))
    row = cursor.fetchone()
    if row[0]:
        cursor.close()
        return
    cursor.execute('insert into messages (message_id) values (?)', (msgid,))
    db.commit()
    cursor.close()
    return

def xref_msgid_with_folder(db, msgid, folder):
    add_msgid_to_db(db, msgid)
    cursor = db.cursor()
    cursor.execute(
        '''
        select
            count(*)
        from
            fldr_msg_xref
        where
            folder_no in (select folder_no from folders where name = ?)
            and
            message_no in (select message_no from messages where message_id = ?)
        ''',
        (folder, msgid)
    )
    row = cursor.fetchone()
    if row[0]:
        cursor.close()
        return
    cursor.execute(
        '''
        insert into
            fldr_msg_xref
            ( folder_no, message_no )
            values
            (
                (select folder_no from folders where name = ?),
                (select message_no from messages where message_id = ?)
            )
        ''',
        (folder, msgid)
    )
    db.commit()
    cursor.close()
    return

def add_msg_meta(db, msgid, imapno, folder):
    add_msgid_to_db(db, msgid)
    cursor = db.cursor()
    cursor.execute(
        '''
        select
            count(*)
        from
            msg_meta
        where
            message_id = ?
            and folder = ?
            and msgno  = ?
        ''',
        (msgid, folder, imapno)
    )
    row = cursor.fetchone()
    if row[0]:
        cursor.close()
        return
    cursor.execute(
        '''
        insert into msg_meta
            (message_id, folder, msgno)
                values
            (?, ?, ?)
        ''',
        (msgid, folder, imapno)
    )
    db.commit()
    cursor.close()
    return

if __name__ == '__main__':
    username = sys.argv[1]
    password = sys.argv[2]
    hostname = 'imap.googlemail.com'
    port = 993
    pp = pprint.PrettyPrinter(indent=4)
    db = sqlite3.connect('test.db')
    header_parser = HeaderParser()

    init_db(db)

    imap = imaplib.IMAP4_SSL(hostname, port)
    if imap is None:
        sys.stderr.write("Unable to open IMAP_SSL connection\n")
        sys.exit(1)

    imap.login(username, password)

    (response, folders) = imap.list()
    if response != 'OK':
        sys.stderr.write('Unable to fetch folder listing\n')
        sys.exit(1)

    folders = [ split_folder_str(folder) for folder in folders if folder.count('HasNoChildren') ]

    for folder in folders:
        (response, data) = imap.select(folder[2])
        count = data[0]
        if response != 'OK':
            sys.stderr.write('Error: Unable to select mailbox %s' % (folder[2]))
            sys.exit(1)

        add_folder_to_db(db, folder[2])

        typ, msgnums = imap.search(None, 'ALL')
        for msgno in msgnums[0].split():
            (response, data) = imap.fetch(msgno, '(BODY[HEADER])')
            if response != 'OK':
                sys.stderr.write('Error: Unable to fetch headers for msg %s\n' % msgno)
                sys.exit(1)

            msg = header_parser.parsestr(data[0][1])
            if 'message-id' not in msg:
                sys.stderr.write('Error: Email has no Message-Id header (msgno:%s)\n' % msgno)
                sys.exit(1)

            xref_msgid_with_folder(db, msg['message-id'], folder[2])
            add_msg_meta(db, msg['message-id'], msgno, folder[2])
