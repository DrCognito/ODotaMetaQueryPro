import pygsheets

gc = pygsheets.authorize(outh_file="client_secret_1032893103395-vtiha2lmocsru6nvc96ipnrjj10lue23.apps.googleusercontent.com.json")

sheet = gc.open("All Results")