#!/usr/bin/env python3
"""
Build the curated quotes.db with difficulty ratings, year, TV/movie flags,
and curated distractors. Run once after edits: python3 build_quotes_db.py
"""

import sqlite3
import json
import os
import re

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "quotes.db")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_quote_config.json")

# ============================================================
# METADATA LOOKUPS
# ============================================================

TV_SHOWS = {
    'Breaking Bad', 'Game of Thrones', 'The Office', 'Friends', 'Stranger Things',
    'The Mandalorian', 'Rick and Morty', 'Parks and Recreation', 'Seinfeld',
    'The Simpsons', 'How I Met Your Mother', 'Brooklyn Nine-Nine',
    "It's Always Sunny in Philadelphia", 'South Park', 'Peaky Blinders',
    'The Wire', 'Dexter', 'The Big Bang Theory', 'Friday Night Lights',
    'Arrested Development', 'Community', 'Psych', 'Downton Abbey',
}

# Release year (or premiere year for TV). One entry per unique title.
MOVIE_YEARS = {
    # AFI / Classic
    "Gone with the Wind": 1939, "The Godfather": 1972, "The Wizard of Oz": 1939,
    "Casablanca": 1942, "Sudden Impact": 1983, "Star Wars": 1977,
    "Taxi Driver": 1976, "Apocalypse Now": 1979, "E.T. the Extra-Terrestrial": 1982,
    "The Sixth Sense": 1999, "A Few Good Men": 1992, "When Harry Met Sally": 1989,
    "Jaws": 1975, "The Terminator": 1984, "Forrest Gump": 1994,
    "Jerry Maguire": 1996, "The Shining": 1980, "Scarface": 1983,
    "The Silence of the Lambs": 1991, "Dr. No": 1962, "Apollo 13": 1995,
    "Terminator 2: Judgment Day": 1991, "Wall Street": 1987,
    "The Godfather Part II": 1974, "The Graduate": 1967,
    "The Adventures of Sherlock Holmes": 1939, "Chinatown": 1974,
    "Goldfinger": 1964, "Top Gun": 1986, "Dirty Dancing": 1987,
    "Titanic": 1997, "Dead Poets Society": 1989, "Frankenstein": 1931,
    "A League of Their Own": 1992, "Psycho": 1960, "Mommie Dearest": 1981,
    "2001: A Space Odyssey": 1968, "Star Trek": 1979, "Citizen Kane": 1941,
    # Star Wars
    "Star Wars: The Empire Strikes Back": 1980, "Star Wars: Return of the Jedi": 1983,
    "Star Wars: The Force Awakens": 2015,
    # Matrix / Fight Club / Pulp Fiction
    "The Matrix": 1999, "Fight Club": 1999, "Pulp Fiction": 1994,
    # Dark Knight
    "The Dark Knight": 2008, "Batman Begins": 2005, "The Dark Knight Rises": 2012,
    # MCU
    "Iron Man": 2008, "Captain America: The First Avenger": 2011,
    "Guardians of the Galaxy": 2014, "The Avengers": 2012,
    "Avengers: Endgame": 2019, "Avengers: Infinity War": 2018,
    "Spider-Man": 2002, "Thor: Ragnarok": 2017, "Black Panther": 2018,
    "Doctor Strange": 2016, "Guardians of the Galaxy Vol. 2": 2017,
    # Harry Potter
    "Harry Potter and the Sorcerer's Stone": 2001,
    "Harry Potter and the Prisoner of Azkaban": 2004,
    "Harry Potter and the Deathly Hallows": 2010,
    "Harry Potter and the Chamber of Secrets": 2002,
    "Harry Potter and the Order of the Phoenix": 2007,
    "Harry Potter and the Deathly Hallows Part 2": 2011,
    # LOTR
    "The Lord of the Rings: The Fellowship of the Ring": 2001,
    "The Lord of the Rings: The Two Towers": 2002,
    "The Lord of the Rings: The Return of the King": 2003,
    # Disney / Pixar / Animated
    "Toy Story": 1995, "Finding Nemo": 2003, "Lilo & Stitch": 2002,
    "Frozen": 2013, "The Lion King": 1994, "Mulan": 1998,
    "Shrek": 2001, "Who Framed Roger Rabbit": 1988, "Ratatouille": 2007,
    "Up": 2009,
    # Comedies
    "Mean Girls": 2004, "Anchorman": 2004, "Napoleon Dynamite": 2004,
    "The Hangover": 2009, "The Hangover Part II": 2011, "Dumb and Dumber": 1994,
    "Dodgeball": 2004, "Jurassic Park": 1993,
    # Action / Thriller
    "John Wick": 2014, "Watchmen": 2009, "Die Hard": 1988,
    "Predator": 1987, "The Princess Bride": 1987, "Back to the Future": 1985,
    "Kingsman: The Secret Service": 2014, "The Big Lebowski": 1998,
    "There Will Be Blood": 2007, "Se7en": 1995, "Captain Phillips": 2013,
    # Drama
    "The Shawshank Redemption": 1994, "National Treasure": 2004,
    "Eternal Sunshine of the Spotless Mind": 2004, "The Wolf of Wall Street": 2013,
    # Pirates
    "Pirates of the Caribbean: The Curse of the Black Pearl": 2003,
    # Horror
    "Carrie": 1976, "Saw": 2004, "Scream": 1996, "Poltergeist": 1982,
    "The Fly": 1986, "It": 2017,
    # TV Shows (premiere year)
    "Breaking Bad": 2008, "Game of Thrones": 2011, "The Office": 2005,
    "Friends": 1994, "Stranger Things": 2016, "The Mandalorian": 2019,
    "Rick and Morty": 2013, "Parks and Recreation": 2009, "Seinfeld": 1989,
    "The Simpsons": 1989, "How I Met Your Mother": 2005, "Brooklyn Nine-Nine": 2013,
    "It's Always Sunny in Philadelphia": 2005, "South Park": 1997,
    "Peaky Blinders": 2013, "The Wire": 2002, "Dexter": 2006,
    "The Big Bang Theory": 2007, "Friday Night Lights": 2006,
    "Arrested Development": 2003, "Community": 2009, "Psych": 2006,
    "Downton Abbey": 2010,
    # Recent / Streaming
    "The Room": 2003, "Gladiator": 2000, "Mad Max: Fury Road": 2015,
    "Inception": 2010, "The Hunger Games": 2012,
    "The Hunger Games: Mockingjay": 2014, "Jane Eyre": 2011,
    "La La Land": 2016, "Office Space": 1999,
}

# Default difficulty is "easy". Only override medium/hard here.
# Key: (quote_text_exact, movie_title_exact) — must match QUOTES list exactly.
DIFFICULTY_OVERRIDES = {
    # ==================== HARD (~18) ====================
    # Old/niche classics most young adults won't recognize
    ("Rosebud.", "Citizen Kane"): "hard",
    ("Forget it, Jake, it's Chinatown.", "Chinatown"): "hard",
    ("No wire hangers, ever!", "Mommie Dearest"): "hard",
    ("Round up the usual suspects.", "Captain Renault", "Casablanca"): "hard",  # will be keyed differently, see below
    ("A boy's best friend is his mother.", "Psycho"): "hard",
    ("It's alive! It's alive!", "Frankenstein"): "hard",
    ("Greed, for lack of a better word, is good.", "Wall Street"): "hard",
    ("Mrs. Robinson, you're trying to seduce me. Aren't you?", "The Graduate"): "hard",
    ("Open the pod bay doors, HAL.", "2001: A Space Odyssey"): "hard",
    ("Elementary, my dear Watson.", "The Adventures of Sherlock Holmes"): "hard",
    ("Here's the thing about the old days: they the old days.", "The Wire"): "hard",
    ("After all, what is a weekend?", "Downton Abbey"): "hard",
    ("I am no bird; and no net ensnares me.", "Jane Eyre"): "hard",
    ("Be afraid. Be very afraid.", "The Fly"): "hard",
    ("They're all gonna laugh at you!", "Carrie"): "hard",
    ("They're heeere.", "Poltergeist"): "hard",
    ("You want it to be one way. But it's the other way.", "The Wire"): "hard",
    ("I'm not bad. I'm just drawn that way.", "Who Framed Roger Rabbit"): "hard",

    # ==================== MEDIUM (~73) ====================
    # Classic/older — recognizable but not for casual young adult viewers
    ("Here's looking at you, kid.", "Casablanca"): "medium",
    ("Go ahead, make my day.", "Sudden Impact"): "medium",
    ("I love the smell of napalm in the morning.", "Apocalypse Now"): "medium",
    ("I'll have what she's having.", "When Harry Met Sally"): "medium",
    ("A census taker once tried to test me. I ate his liver with some fava beans and a nice Chianti.", "The Silence of the Lambs"): "medium",
    ("Keep your friends close, but your enemies closer.", "The Godfather Part II"): "medium",
    ("Of all the gin joints in all the towns in all the world, she walks into mine.", "Casablanca"): "medium",
    ("Nobody puts Baby in a corner.", "Dirty Dancing"): "medium",
    ("Carpe diem. Seize the day, boys. Make your lives extraordinary.", "Dead Poets Society"): "medium",
    ("There's no crying in baseball!", "A League of Their Own"): "medium",
    ("Live long and prosper.", "Star Trek"): "medium",
    ("I'll get you, my pretty, and your little dog too!", "The Wizard of Oz"): "medium",
    # Matrix deep cuts
    ("What is the Matrix? Control.", "The Matrix"): "medium",
    ("Unfortunately, no one can be told what the Matrix is. You have to see it for yourself.", "The Matrix"): "medium",
    ("What if I told you everything you know is a lie?", "The Matrix"): "medium",
    # Fight Club deep cuts
    ("It's only after we've lost everything that we're free to do anything.", "Fight Club"): "medium",
    ("I am Jack's complete lack of surprise.", "Fight Club"): "medium",
    ("You are not your job. You are not how much money you have in the bank.", "Fight Club"): "medium",
    ("We buy things we don't need, with money we don't have, to impress people we don't like.", "Fight Club"): "medium",
    ("The things you own end up owning you.", "Fight Club"): "medium",
    # Pulp Fiction — fan favorites
    ("That's a pretty f***in' good milkshake.", "Pulp Fiction"): "medium",
    # Dark Knight deep cut
    ("I believe whatever doesn't kill you simply makes you... stranger.", "The Dark Knight"): "medium",
    # MCU less iconic
    ("Proof that Tony Stark has a heart.", "Iron Man"): "medium",
    ("In time, you will know what it's like to lose.", "Avengers: Infinity War"): "medium",
    # Harry Potter — requires being a fan
    ("I must not tell lies.", "Harry Potter and the Order of the Phoenix"): "medium",
    ("Fear of a name increases fear of the thing itself.", "Harry Potter and the Chamber of Secrets"): "medium",
    ("The boy who lived has come to die.", "Harry Potter and the Deathly Hallows Part 2"): "medium",
    # LOTR — generic-sounding deep cuts
    ("Even the smallest person can change the course of the future.", "The Lord of the Rings: The Fellowship of the Ring"): "medium",
    ("I would have followed you, my brother. My captain. My king.", "The Lord of the Rings: The Fellowship of the Ring"): "medium",
    ("All we have to decide is what to do with the time that is given us.", "The Lord of the Rings: The Fellowship of the Ring"): "medium",
    ("The board is set, the pieces are moving.", "The Lord of the Rings: The Return of the King"): "medium",
    ("That still only counts as one!", "The Lord of the Rings: The Return of the King"): "medium",
    # Disney/Pixar — less iconic
    ("The flower that blooms in adversity is the most rare and beautiful of all.", "Mulan"): "medium",
    ("Not everyone can become a great artist, but a great artist can come from anywhere.", "Ratatouille"): "medium",
    ("Fish are friends, not food.", "Finding Nemo"): "medium",
    ("Adventure is out there!", "Up"): "medium",
    ("Some of you may die, but that is a sacrifice I am willing to make.", "Shrek"): "medium",
    # Comedies — lesser known
    ("Do the chickens have large talons?", "Napoleon Dynamite"): "medium",
    ("What are you gonna do, stab me? — Quote from man stabbed.", "The Hangover"): "medium",
    ("We're the three best friends that anyone could have.", "The Hangover"): "medium",
    ("But did you die?", "The Hangover Part II"): "medium",
    ("I like that boulder. That is a nice boulder.", "Shrek"): "medium",
    ("It's a bold strategy, Cotton. Let's see if it pays off for 'em.", "Dodgeball"): "medium",
    # Action/Thriller deep cuts
    ("Have fun storming the castle!", "The Princess Bride"): "medium",
    ("You keep using that word. I do not think it means what you think it means.", "The Princess Bride"): "medium",
    ("This is heavy.", "Back to the Future"): "medium",
    ("That's just, like, your opinion, man.", "The Big Lebowski"): "medium",
    ("Nobody f***s with the Jesus.", "The Big Lebowski"): "medium",
    ("I drink your milkshake! I drink it up!", "There Will Be Blood"): "medium",
    # Drama
    ("Why do I fall in love with every woman I see who shows me the least bit of attention?", "Eternal Sunshine of the Spotless Mind"): "medium",
    ("I'm not leaving!", "The Wolf of Wall Street"): "medium",
    ("You bow to no one.", "The Lord of the Rings: The Return of the King"): "medium",
    # Pirates — generic-sounding
    ("Not all treasure is silver and gold, mate.", "Pirates of the Caribbean: The Curse of the Black Pearl"): "medium",
    ("The problem is not the problem. The problem is your attitude about the problem.", "Pirates of the Caribbean: The Curse of the Black Pearl"): "medium",
    # Horror
    ("I want to play a game.", "Saw"): "medium",
    # TV — deeper cuts that require being a fan
    ("This is my own private domicile, and I will not be harassed. B*tch!", "Breaking Bad"): "medium",
    ("Chaos isn't a pit. Chaos is a ladder.", "Game of Thrones"): "medium",
    ("Any man who must say 'I am the king' is no true king.", "Game of Thrones"): "medium",
    ("Tell Cersei. I want her to know it was me.", "Game of Thrones"): "medium",
    ("The lone wolf dies, but the pack survives.", "Game of Thrones"): "medium",
    ("He's her lobster.", "Friends"): "medium",
    ("It's a moo point. It's like a cow's opinion, it just doesn't matter.", "Friends"): "medium",
    ("Unagi.", "Friends"): "medium",
    ("Mornings are for coffee and contemplation.", "Stranger Things"): "medium",
    ("It's finger-lickin' good. Kentucky Fried Chicken. I'll take you there.", "Stranger Things"): "medium",
    ("Grogu and I can feel each other's thoughts.", "The Mandalorian"): "medium",
    ("To live is to risk it all. Otherwise you're just an inert chunk of randomly assembled molecules.", "Rick and Morty"): "medium",
    ("Sometimes science is more art than science.", "Rick and Morty"): "medium",
    ("I once worked with a guy for three years and never learned his name.", "Parks and Recreation"): "medium",
    ("Not that there's anything wrong with that.", "Seinfeld"): "medium",
    ("Serenity now!", "Seinfeld"): "medium",
    ("I was in the pool!", "Seinfeld"): "medium",
    ("I, for one, welcome our new insect overlords.", "The Simpsons"): "medium",
    ("Nothing good ever happens after 2 a.m.", "How I Met Your Mother"): "medium",
    ("Everything is garbage. Never love anything.", "Brooklyn Nine-Nine"): "medium",
    ("I haven't even begun to peak.", "It's Always Sunny in Philadelphia"): "medium",
    ("Stupid science b*tch couldn't even make I more smarter.", "It's Always Sunny in Philadelphia"): "medium",
    ("The implication.", "It's Always Sunny in Philadelphia"): "medium",
    ("I'm not a traitor to my class. I am just an extreme example of what a working man can achieve.", "Peaky Blinders"): "medium",
    ("Everyone's a whore, Grace. We just sell different parts of ourselves.", "Peaky Blinders"): "medium",
    ("I don't pay for suits. My suits are on the house.", "Peaky Blinders"): "medium",
    ("All in the game, yo. All in the game.", "The Wire"): "medium",
    ("An idea is like a virus, resilient, highly contagious.", "Inception"): "medium",
    ("Here's to the ones who dream, foolish as they may seem.", "La La Land"): "medium",
    ("Oh hi, Mark.", "The Room"): "medium",
}

# Curated wrong answers for hard/tricky questions where the algorithm would fail.
# Key: (quote_text_exact, movie_title_exact)
# Value: {"movie": [3 wrong movie titles], "character": [3 wrong character names]}
CURATED_DISTRACTORS = {
    ("Rosebud.", "Citizen Kane"): {
        "movie": ["Sunset Boulevard", "The Third Man", "12 Angry Men"],
        "character": ["Norma Desmond", "Philip Marlowe", "Atticus Finch"],
    },
    ("Forget it, Jake, it's Chinatown.", "Chinatown"): {
        "movie": ["Sunset Boulevard", "The Maltese Falcon", "L.A. Confidential"],
        "character": ["Sam Spade", "Philip Marlowe", "Ed Exley"],
    },
    ("No wire hangers, ever!", "Mommie Dearest"): {
        "movie": ["Whatever Happened to Baby Jane?", "The Hours", "Black Swan"],
        "character": ["Norma Desmond", "Baby Jane Hudson", "Nina Sayers"],
    },
    ("A boy's best friend is his mother.", "Psycho"): {
        "movie": ["The Silence of the Lambs", "Se7en", "Misery"],
        "character": ["Hannibal Lecter", "John Doe", "Annie Wilkes"],
    },
    ("It's alive! It's alive!", "Frankenstein"): {
        "movie": ["Dracula", "The Invisible Man", "Dr. Jekyll and Mr. Hyde"],
        "character": ["Count Dracula", "Dr. Griffin", "Dr. Jekyll"],
    },
    ("Greed, for lack of a better word, is good.", "Wall Street"): {
        "movie": ["The Wolf of Wall Street", "American Psycho", "The Big Short"],
        "character": ["Jordan Belfort", "Patrick Bateman", "Mark Baum"],
    },
    ("Mrs. Robinson, you're trying to seduce me. Aren't you?", "The Graduate"): {
        "movie": ["American Beauty", "The Big Lebowski", "Risky Business"],
        "character": ["Lester Burnham", "Joel Goodson", "Don Draper"],
    },
    ("Open the pod bay doors, HAL.", "2001: A Space Odyssey"): {
        "movie": ["Alien", "Interstellar", "Blade Runner"],
        "character": ["Ellen Ripley", "Cooper", "Rick Deckard"],
    },
    ("Elementary, my dear Watson.", "The Adventures of Sherlock Holmes"): {
        "movie": ["Sherlock Holmes", "Murder on the Orient Express", "Knives Out"],
        "character": ["Robert Downey Jr. Holmes", "Hercule Poirot", "Benoit Blanc"],
    },
    ("Here's the thing about the old days: they the old days.", "The Wire"): {
        "movie": ["The Sopranos", "Boardwalk Empire", "Power"],
        "character": ["Tony Soprano", "Nucky Thompson", "James St. Patrick"],
    },
    ("After all, what is a weekend?", "Downton Abbey"): {
        "movie": ["The Crown", "Bridgerton", "Gosford Park"],
        "character": ["Queen Elizabeth", "Lady Whistledown", "Lady Trentham"],
    },
    ("I am no bird; and no net ensnares me.", "Jane Eyre"): {
        "movie": ["Pride and Prejudice", "Wuthering Heights", "Little Women"],
        "character": ["Elizabeth Bennet", "Catherine Earnshaw", "Jo March"],
    },
    ("Be afraid. Be very afraid.", "The Fly"): {
        "movie": ["Alien", "The Thing", "Invasion of the Body Snatchers"],
        "character": ["Ellen Ripley", "MacReady", "Dr. Miles Bennell"],
    },
    ("They're all gonna laugh at you!", "Carrie"): {
        "movie": ["The Exorcist", "Rosemary's Baby", "The Omen"],
        "character": ["Regan's Mother", "Rosemary", "Robert Thorn"],
    },
    ("They're heeere.", "Poltergeist"): {
        "movie": ["The Shining", "The Exorcist", "The Conjuring"],
        "character": ["Danny Torrance", "Regan MacNeil", "Lorraine Warren"],
    },
    ("You want it to be one way. But it's the other way.", "The Wire"): {
        "movie": ["The Sopranos", "Breaking Bad", "Narcos"],
        "character": ["Tony Soprano", "Gustavo Fring", "Pablo Escobar"],
    },
    ("I'm not bad. I'm just drawn that way.", "Who Framed Roger Rabbit"): {
        "movie": ["Cool World", "Space Jam", "Enchanted"],
        "character": ["Holli Would", "Lola Bunny", "Giselle"],
    },
    ("I drink your milkshake! I drink it up!", "There Will Be Blood"): {
        "movie": ["No Country for Old Men", "The Revenant", "Gangs of New York"],
        "character": ["Anton Chigurh", "Hugh Glass", "Bill the Butcher"],
    },
    # Medium quotes that would get bad distractors from the algorithm
    ("Oh hi, Mark.", "The Room"): {
        "movie": ["Birdman", "Disaster Artist", "Napoleon Dynamite"],
        "character": ["Riggan Thomson", "Greg Sestero", "Uncle Rico"],
    },
    ("Chaos isn't a pit. Chaos is a ladder.", "Game of Thrones"): {
        "movie": ["House of the Dragon", "Vikings", "The Witcher"],
        "character": ["Otto Hightower", "Ragnar Lothbrok", "Emhyr var Emreis"],
    },
    ("I am Jack's complete lack of surprise.", "Fight Club"): {
        "movie": ["American Psycho", "Trainspotting", "A Clockwork Orange"],
        "character": ["Patrick Bateman", "Mark Renton", "Alex DeLarge"],
    },
}


# ============================================================
# CURATED ICONIC QUOTES
# Format: (quote, character, title, genre)
# Difficulty, year, is_tv, and curated distractors are looked up
# from the dicts above during build.
# ============================================================

QUOTES = [
    # ========== AFI 100 GREATEST (filtered for young adult recognition) ==========
    ("Frankly, my dear, I don't give a damn.", "Rhett Butler", "Gone with the Wind", "Drama, Romance"),
    ("I'm gonna make him an offer he can't refuse.", "Vito Corleone", "The Godfather", "Crime, Drama"),
    ("Toto, I've a feeling we're not in Kansas anymore.", "Dorothy Gale", "The Wizard of Oz", "Adventure, Family, Fantasy"),
    ("Here's looking at you, kid.", "Rick Blaine", "Casablanca", "Drama, Romance, War"),
    ("Go ahead, make my day.", "Harry Callahan", "Sudden Impact", "Action, Crime, Thriller"),
    ("May the Force be with you.", "Han Solo", "Star Wars", "Action, Adventure, Sci-Fi"),
    ("You talkin' to me?", "Travis Bickle", "Taxi Driver", "Crime, Drama"),
    ("I love the smell of napalm in the morning.", "Lt. Col. Bill Kilgore", "Apocalypse Now", "Drama, War"),
    ("E.T. phone home.", "E.T.", "E.T. the Extra-Terrestrial", "Adventure, Family, Sci-Fi"),
    ("I see dead people.", "Cole Sear", "The Sixth Sense", "Drama, Mystery, Thriller"),
    ("You can't handle the truth!", "Col. Nathan Jessup", "A Few Good Men", "Drama, Thriller"),
    ("I'll have what she's having.", "Customer", "When Harry Met Sally", "Comedy, Drama, Romance"),
    ("You're gonna need a bigger boat.", "Martin Brody", "Jaws", "Adventure, Thriller"),
    ("I'll be back.", "The Terminator", "The Terminator", "Action, Sci-Fi"),
    ("My mama always said life was like a box of chocolates. You never know what you're gonna get.", "Forrest Gump", "Forrest Gump", "Drama, Romance"),
    ("Show me the money!", "Rod Tidwell", "Jerry Maguire", "Comedy, Drama, Romance, Sport"),
    ("You had me at hello.", "Dorothy Boyd", "Jerry Maguire", "Comedy, Drama, Romance, Sport"),
    ("There's no place like home.", "Dorothy Gale", "The Wizard of Oz", "Adventure, Family, Fantasy"),
    ("Here's Johnny!", "Jack Torrance", "The Shining", "Drama, Horror"),
    ("Say hello to my little friend!", "Tony Montana", "Scarface", "Crime, Drama"),
    ("A census taker once tried to test me. I ate his liver with some fava beans and a nice Chianti.", "Hannibal Lecter", "The Silence of the Lambs", "Crime, Drama, Thriller"),
    ("Bond. James Bond.", "James Bond", "Dr. No", "Action, Adventure, Thriller"),
    ("Houston, we have a problem.", "Jim Lovell", "Apollo 13", "Adventure, Drama"),
    ("Hasta la vista, baby.", "The Terminator", "Terminator 2: Judgment Day", "Action, Sci-Fi"),
    ("Greed, for lack of a better word, is good.", "Gordon Gekko", "Wall Street", "Crime, Drama"),
    ("Keep your friends close, but your enemies closer.", "Michael Corleone", "The Godfather Part II", "Crime, Drama"),
    ("Mrs. Robinson, you're trying to seduce me. Aren't you?", "Benjamin Braddock", "The Graduate", "Comedy, Drama, Romance"),
    ("Elementary, my dear Watson.", "Sherlock Holmes", "The Adventures of Sherlock Holmes", "Adventure, Crime, Mystery"),
    ("Forget it, Jake, it's Chinatown.", "Lawrence Walsh", "Chinatown", "Drama, Mystery, Thriller"),
    ("Of all the gin joints in all the towns in all the world, she walks into mine.", "Rick Blaine", "Casablanca", "Drama, Romance, War"),
    ("A martini. Shaken, not stirred.", "James Bond", "Goldfinger", "Action, Adventure, Thriller"),
    ("I feel the need — the need for speed!", "Maverick", "Top Gun", "Action, Drama"),
    ("Nobody puts Baby in a corner.", "Johnny Castle", "Dirty Dancing", "Drama, Music, Romance"),
    ("I'm the king of the world!", "Jack Dawson", "Titanic", "Drama, Romance"),
    ("Carpe diem. Seize the day, boys. Make your lives extraordinary.", "John Keating", "Dead Poets Society", "Comedy, Drama"),
    ("My precious.", "Gollum", "The Lord of the Rings: The Two Towers", "Adventure, Drama, Fantasy"),
    ("It's alive! It's alive!", "Henry Frankenstein", "Frankenstein", "Drama, Horror, Sci-Fi"),
    ("There's no crying in baseball!", "Jimmy Dugan", "A League of Their Own", "Comedy, Drama, Sport"),
    ("A boy's best friend is his mother.", "Norman Bates", "Psycho", "Horror, Mystery, Thriller"),
    ("Round up the usual suspects.", "Captain Renault", "Casablanca", "Drama, Romance, War"),
    ("I'll get you, my pretty, and your little dog too!", "Wicked Witch", "The Wizard of Oz", "Adventure, Family, Fantasy"),
    ("No wire hangers, ever!", "Joan Crawford", "Mommie Dearest", "Biography, Drama"),
    ("Open the pod bay doors, HAL.", "Dave Bowman", "2001: A Space Odyssey", "Adventure, Sci-Fi"),
    ("Live long and prosper.", "Spock", "Star Trek", "Adventure, Sci-Fi"),
    ("Rosebud.", "Charles Foster Kane", "Citizen Kane", "Drama, Mystery"),

    # ========== STAR WARS ==========
    ("Do, or do not. There is no try.", "Yoda", "Star Wars: The Empire Strikes Back", "Action, Adventure, Sci-Fi"),
    ("I am your father.", "Darth Vader", "Star Wars: The Empire Strikes Back", "Action, Adventure, Sci-Fi"),
    ("It's a trap!", "Admiral Ackbar", "Star Wars: Return of the Jedi", "Action, Adventure, Sci-Fi"),
    ("I've got a bad feeling about this.", "Han Solo", "Star Wars", "Action, Adventure, Sci-Fi"),
    ("These aren't the droids you're looking for.", "Obi-Wan Kenobi", "Star Wars", "Action, Adventure, Sci-Fi"),
    ("Help me, Obi-Wan Kenobi. You're my only hope.", "Princess Leia", "Star Wars", "Action, Adventure, Sci-Fi"),
    ("Never tell me the odds!", "Han Solo", "Star Wars: The Empire Strikes Back", "Action, Adventure, Sci-Fi"),
    ("I find your lack of faith disturbing.", "Darth Vader", "Star Wars", "Action, Adventure, Sci-Fi"),
    ("The Force will be with you. Always.", "Obi-Wan Kenobi", "Star Wars", "Action, Adventure, Sci-Fi"),
    ("Chewie, we're home.", "Han Solo", "Star Wars: The Force Awakens", "Action, Adventure, Sci-Fi"),
    ("That's no moon. It's a space station.", "Obi-Wan Kenobi", "Star Wars", "Action, Adventure, Sci-Fi"),
    ("In my experience, there's no such thing as luck.", "Obi-Wan Kenobi", "Star Wars", "Action, Adventure, Sci-Fi"),

    # ========== THE MATRIX ==========
    ("There is no spoon.", "Spoon Boy", "The Matrix", "Action, Sci-Fi"),
    ("I know kung fu.", "Neo", "The Matrix", "Action, Sci-Fi"),
    ("Welcome to the real world.", "Morpheus", "The Matrix", "Action, Sci-Fi"),
    ("What is the Matrix? Control.", "Morpheus", "The Matrix", "Action, Sci-Fi"),
    ("Unfortunately, no one can be told what the Matrix is. You have to see it for yourself.", "Morpheus", "The Matrix", "Action, Sci-Fi"),
    ("You take the blue pill, the story ends. You take the red pill, you stay in Wonderland, and I show you how deep the rabbit hole goes.", "Morpheus", "The Matrix", "Action, Sci-Fi"),
    ("Dodge this.", "Trinity", "The Matrix", "Action, Sci-Fi"),
    ("Mr. Anderson.", "Agent Smith", "The Matrix", "Action, Sci-Fi"),
    ("What if I told you everything you know is a lie?", "Morpheus", "The Matrix", "Action, Sci-Fi"),

    # ========== FIGHT CLUB ==========
    ("The first rule of Fight Club is: you do not talk about Fight Club.", "Tyler Durden", "Fight Club", "Drama"),
    ("It's only after we've lost everything that we're free to do anything.", "Tyler Durden", "Fight Club", "Drama"),
    ("I am Jack's complete lack of surprise.", "The Narrator", "Fight Club", "Drama"),
    ("You are not your job. You are not how much money you have in the bank.", "Tyler Durden", "Fight Club", "Drama"),
    ("His name is Robert Paulson.", "Space Monkey", "Fight Club", "Drama"),
    ("I want you to hit me as hard as you can.", "Tyler Durden", "Fight Club", "Drama"),

    # ========== PULP FICTION ==========
    ("Say 'what' again. Say 'what' again, I dare you, I double dare you.", "Jules Winnfield", "Pulp Fiction", "Crime, Drama"),
    ("Zed's dead, baby. Zed's dead.", "Butch Coolidge", "Pulp Fiction", "Crime, Drama"),
    ("English, motherf***er, do you speak it?", "Jules Winnfield", "Pulp Fiction", "Crime, Drama"),
    ("They call it a Royale with cheese.", "Vincent Vega", "Pulp Fiction", "Crime, Drama"),
    ("That's a pretty f***in' good milkshake.", "Vincent Vega", "Pulp Fiction", "Crime, Drama"),

    # ========== THE DARK KNIGHT TRILOGY ==========
    ("Why so serious?", "The Joker", "The Dark Knight", "Action, Crime, Drama"),
    ("You either die a hero or you live long enough to see yourself become the villain.", "Harvey Dent", "The Dark Knight", "Action, Crime, Drama"),
    ("Some men just want to watch the world burn.", "Alfred Pennyworth", "The Dark Knight", "Action, Crime, Drama"),
    ("It's not who I am underneath, but what I do that defines me.", "Batman", "Batman Begins", "Action, Adventure"),
    ("I'm not a monster. I'm just ahead of the curve.", "The Joker", "The Dark Knight", "Action, Crime, Drama"),
    ("You wanna know how I got these scars?", "The Joker", "The Dark Knight", "Action, Crime, Drama"),
    ("I believe whatever doesn't kill you simply makes you... stranger.", "The Joker", "The Dark Knight", "Action, Crime, Drama"),
    ("When Gotham is ashes, you have my permission to die.", "Bane", "The Dark Knight Rises", "Action, Thriller"),

    # ========== MCU / MARVEL ==========
    ("I am Iron Man.", "Tony Stark", "Iron Man", "Action, Adventure, Sci-Fi"),
    ("I can do this all day.", "Steve Rogers", "Captain America: The First Avenger", "Action, Adventure, Sci-Fi"),
    ("I am Groot.", "Groot", "Guardians of the Galaxy", "Action, Adventure, Comedy"),
    ("That's my secret, Captain. I'm always angry.", "Bruce Banner", "The Avengers", "Action, Adventure, Sci-Fi"),
    ("Puny god.", "Hulk", "The Avengers", "Action, Adventure, Sci-Fi"),
    ("I am inevitable.", "Thanos", "Avengers: Endgame", "Action, Adventure, Sci-Fi"),
    ("And I... am... Iron Man.", "Tony Stark", "Avengers: Endgame", "Action, Adventure, Sci-Fi"),
    ("Dread it. Run from it. Destiny arrives all the same.", "Thanos", "Avengers: Infinity War", "Action, Adventure, Sci-Fi"),
    ("Mr. Stark, I don't feel so good.", "Peter Parker", "Avengers: Infinity War", "Action, Adventure, Sci-Fi"),
    ("Avengers, assemble.", "Steve Rogers", "Avengers: Endgame", "Action, Adventure, Sci-Fi"),
    ("On your left.", "Sam Wilson", "Avengers: Endgame", "Action, Adventure, Sci-Fi"),
    ("With great power comes great responsibility.", "Uncle Ben", "Spider-Man", "Action, Adventure, Sci-Fi"),
    ("I love you 3000.", "Morgan Stark", "Avengers: Endgame", "Action, Adventure, Sci-Fi"),
    ("He's a friend from work!", "Thor", "Thor: Ragnarok", "Action, Adventure, Comedy"),
    ("We have a Hulk.", "Tony Stark", "The Avengers", "Action, Adventure, Sci-Fi"),
    ("Wakanda forever!", "T'Challa", "Black Panther", "Action, Adventure, Sci-Fi"),
    ("Perfectly balanced, as all things should be.", "Thanos", "Avengers: Infinity War", "Action, Adventure, Sci-Fi"),
    ("In time, you will know what it's like to lose.", "Thanos", "Avengers: Infinity War", "Action, Adventure, Sci-Fi"),
    ("Proof that Tony Stark has a heart.", "Pepper Potts", "Iron Man", "Action, Adventure, Sci-Fi"),

    # ========== HARRY POTTER ==========
    ("It's leviOsa, not levioSA.", "Hermione Granger", "Harry Potter and the Sorcerer's Stone", "Adventure, Family, Fantasy"),
    ("I solemnly swear that I am up to no good.", "Harry Potter", "Harry Potter and the Prisoner of Azkaban", "Adventure, Family, Fantasy"),
    ("After all this time? Always.", "Severus Snape", "Harry Potter and the Deathly Hallows", "Adventure, Drama, Fantasy"),
    ("It does not do to dwell on dreams and forget to live.", "Albus Dumbledore", "Harry Potter and the Sorcerer's Stone", "Adventure, Family, Fantasy"),
    ("Happiness can be found even in the darkest of times, if one only remembers to turn on the light.", "Albus Dumbledore", "Harry Potter and the Prisoner of Azkaban", "Adventure, Family, Fantasy"),
    ("You're a wizard, Harry.", "Hagrid", "Harry Potter and the Sorcerer's Stone", "Adventure, Family, Fantasy"),
    ("I must not tell lies.", "Harry Potter", "Harry Potter and the Order of the Phoenix", "Adventure, Family, Fantasy"),
    ("Mischief managed.", "Harry Potter", "Harry Potter and the Prisoner of Azkaban", "Adventure, Family, Fantasy"),
    ("Expecto Patronum!", "Harry Potter", "Harry Potter and the Prisoner of Azkaban", "Adventure, Family, Fantasy"),
    ("Not my daughter, you b*tch!", "Molly Weasley", "Harry Potter and the Deathly Hallows Part 2", "Adventure, Drama, Fantasy"),
    ("Ten points to Gryffindor!", "Albus Dumbledore", "Harry Potter and the Sorcerer's Stone", "Adventure, Family, Fantasy"),
    ("The boy who lived has come to die.", "Voldemort", "Harry Potter and the Deathly Hallows Part 2", "Adventure, Drama, Fantasy"),
    ("Fear of a name increases fear of the thing itself.", "Hermione Granger", "Harry Potter and the Chamber of Secrets", "Adventure, Family, Fantasy"),

    # ========== LORD OF THE RINGS ==========
    ("One does not simply walk into Mordor.", "Boromir", "The Lord of the Rings: The Fellowship of the Ring", "Adventure, Drama, Fantasy"),
    ("You shall not pass!", "Gandalf", "The Lord of the Rings: The Fellowship of the Ring", "Adventure, Drama, Fantasy"),
    ("A wizard is never late. He arrives precisely when he means to.", "Gandalf", "The Lord of the Rings: The Fellowship of the Ring", "Adventure, Drama, Fantasy"),
    ("There's some good in this world, Mr. Frodo, and it's worth fighting for.", "Samwise Gamgee", "The Lord of the Rings: The Two Towers", "Adventure, Drama, Fantasy"),
    ("Even the smallest person can change the course of the future.", "Galadriel", "The Lord of the Rings: The Fellowship of the Ring", "Adventure, Drama, Fantasy"),
    ("I would have followed you, my brother. My captain. My king.", "Boromir", "The Lord of the Rings: The Fellowship of the Ring", "Adventure, Drama, Fantasy"),
    ("That still only counts as one!", "Gimli", "The Lord of the Rings: The Return of the King", "Adventure, Drama, Fantasy"),
    ("All we have to decide is what to do with the time that is given us.", "Gandalf", "The Lord of the Rings: The Fellowship of the Ring", "Adventure, Drama, Fantasy"),
    ("The board is set, the pieces are moving.", "Gandalf", "The Lord of the Rings: The Return of the King", "Adventure, Drama, Fantasy"),
    ("I am no man.", "Eowyn", "The Lord of the Rings: The Return of the King", "Adventure, Drama, Fantasy"),

    # ========== DISNEY / PIXAR / ANIMATED ==========
    ("To infinity and beyond!", "Buzz Lightyear", "Toy Story", "Animation, Adventure, Comedy"),
    ("Just keep swimming.", "Dory", "Finding Nemo", "Animation, Adventure, Comedy"),
    ("Ohana means family. Family means nobody gets left behind or forgotten.", "Stitch", "Lilo & Stitch", "Animation, Adventure, Comedy"),
    ("Let it go, let it go, can't hold it back anymore.", "Elsa", "Frozen", "Animation, Adventure, Comedy"),
    ("You've got a friend in me.", "Woody", "Toy Story", "Animation, Adventure, Comedy"),
    ("I'm surrounded by idiots.", "Scar", "The Lion King", "Animation, Adventure, Drama"),
    ("Hakuna Matata! It means no worries.", "Timon", "The Lion King", "Animation, Adventure, Drama"),
    ("Everything the light touches is our kingdom.", "Mufasa", "The Lion King", "Animation, Adventure, Drama"),
    ("Remember who you are.", "Mufasa", "The Lion King", "Animation, Adventure, Drama"),
    ("Long live the king.", "Scar", "The Lion King", "Animation, Adventure, Drama"),
    ("The flower that blooms in adversity is the most rare and beautiful of all.", "The Emperor", "Mulan", "Animation, Adventure, Comedy"),
    ("Ogres are like onions. They have layers.", "Shrek", "Shrek", "Animation, Adventure, Comedy"),
    ("Some of you may die, but that is a sacrifice I am willing to make.", "Lord Farquaad", "Shrek", "Animation, Adventure, Comedy"),
    ("That'll do, Donkey. That'll do.", "Shrek", "Shrek", "Animation, Adventure, Comedy"),
    ("I'm not bad. I'm just drawn that way.", "Jessica Rabbit", "Who Framed Roger Rabbit", "Animation, Comedy, Crime"),
    ("Fish are friends, not food.", "Bruce", "Finding Nemo", "Animation, Adventure, Comedy"),
    ("Not everyone can become a great artist, but a great artist can come from anywhere.", "Anton Ego", "Ratatouille", "Animation, Comedy, Family"),
    ("Adventure is out there!", "Charles Muntz", "Up", "Animation, Adventure, Comedy"),
    ("You are a toy!", "Woody", "Toy Story", "Animation, Adventure, Comedy"),

    # ========== COMEDIES (Young Adult Staples) ==========
    ("That's so fetch.", "Gretchen Wieners", "Mean Girls", "Comedy"),
    ("On Wednesdays we wear pink.", "Karen Smith", "Mean Girls", "Comedy"),
    ("You can't sit with us!", "Gretchen Wieners", "Mean Girls", "Comedy"),
    ("She doesn't even go here!", "Damian", "Mean Girls", "Comedy"),
    ("Get in loser, we're going shopping.", "Regina George", "Mean Girls", "Comedy"),
    ("The limit does not exist!", "Cady Heron", "Mean Girls", "Comedy"),
    ("Stop trying to make fetch happen. It's not going to happen.", "Regina George", "Mean Girls", "Comedy"),
    ("I'm not a regular mom, I'm a cool mom.", "Mrs. George", "Mean Girls", "Comedy"),
    ("That is the ugliest effing skirt I have ever seen.", "Regina George", "Mean Girls", "Comedy"),
    ("60% of the time, it works every time.", "Brian Fantana", "Anchorman", "Comedy"),
    ("I'm kind of a big deal.", "Ron Burgundy", "Anchorman", "Comedy"),
    ("I'm in a glass case of emotion!", "Ron Burgundy", "Anchorman", "Comedy"),
    ("Stay classy, San Diego.", "Ron Burgundy", "Anchorman", "Comedy"),
    ("Vote for Pedro.", "Pedro", "Napoleon Dynamite", "Comedy"),
    ("Gosh! Tina, you fat lard, come get some dinner!", "Napoleon Dynamite", "Napoleon Dynamite", "Comedy"),
    ("Do the chickens have large talons?", "Napoleon Dynamite", "Napoleon Dynamite", "Comedy"),
    ("What are you gonna do, stab me? — Quote from man stabbed.", "Phil", "The Hangover", "Comedy"),
    ("We're the three best friends that anyone could have.", "Alan Garner", "The Hangover", "Comedy"),
    ("But did you die?", "Mr. Chow", "The Hangover Part II", "Comedy"),
    ("So you're telling me there's a chance.", "Lloyd Christmas", "Dumb and Dumber", "Comedy"),
    ("I like that boulder. That is a nice boulder.", "Donkey", "Shrek", "Animation, Adventure, Comedy"),
    ("It's a bold strategy, Cotton. Let's see if it pays off for 'em.", "Pepper Brooks", "Dodgeball", "Comedy, Sport"),
    ("Life, uh, finds a way.", "Dr. Ian Malcolm", "Jurassic Park", "Action, Adventure, Sci-Fi"),
    ("Clever girl.", "Robert Muldoon", "Jurassic Park", "Action, Adventure, Sci-Fi"),
    ("Hold on to your butts.", "Ray Arnold", "Jurassic Park", "Action, Adventure, Sci-Fi"),
    ("Welcome to Jurassic Park.", "John Hammond", "Jurassic Park", "Action, Adventure, Sci-Fi"),

    # ========== ACTION / THRILLER ==========
    ("Why do we fall? So we can learn to pick ourselves up.", "Alfred Pennyworth", "Batman Begins", "Action, Adventure"),
    ("The name's Wick. John Wick.", "John Wick", "John Wick", "Action, Thriller"),
    ("I'm not locked in here with you. You're locked in here with me!", "Rorschach", "Watchmen", "Action, Drama, Sci-Fi"),
    ("Yippee-ki-yay, motherf***er.", "John McClane", "Die Hard", "Action, Thriller"),
    ("Get to the chopper!", "Dutch", "Predator", "Action, Sci-Fi, Thriller"),
    ("Run, Forrest, run!", "Jenny Curran", "Forrest Gump", "Drama, Romance"),
    ("After you.", "Inigo Montoya", "The Princess Bride", "Adventure, Comedy, Family"),
    ("My name is Inigo Montoya. You killed my father. Prepare to die.", "Inigo Montoya", "The Princess Bride", "Adventure, Comedy, Family"),
    ("As you wish.", "Westley", "The Princess Bride", "Adventure, Comedy, Family"),
    ("Inconceivable!", "Vizzini", "The Princess Bride", "Adventure, Comedy, Family"),
    ("Have fun storming the castle!", "Miracle Max", "The Princess Bride", "Adventure, Comedy, Family"),
    ("You keep using that word. I do not think it means what you think it means.", "Inigo Montoya", "The Princess Bride", "Adventure, Comedy, Family"),
    ("Roads? Where we're going, we don't need roads.", "Doc Brown", "Back to the Future", "Adventure, Comedy, Sci-Fi"),
    ("Great Scott!", "Doc Brown", "Back to the Future", "Adventure, Comedy, Sci-Fi"),
    ("This is heavy.", "Marty McFly", "Back to the Future", "Adventure, Comedy, Sci-Fi"),
    ("Manners. Maketh. Man.", "Harry Hart", "Kingsman: The Secret Service", "Action, Adventure, Comedy"),
    ("I'm the Dude. So that's what you call me.", "The Dude", "The Big Lebowski", "Comedy, Crime"),
    ("The Dude abides.", "The Dude", "The Big Lebowski", "Comedy, Crime"),
    ("That's just, like, your opinion, man.", "The Dude", "The Big Lebowski", "Comedy, Crime"),
    ("Nobody f***s with the Jesus.", "Jesus Quintana", "The Big Lebowski", "Comedy, Crime"),
    ("I drink your milkshake! I drink it up!", "Daniel Plainview", "There Will Be Blood", "Drama"),
    ("What's in the box?!", "David Mills", "Se7en", "Crime, Drama, Mystery"),
    ("I'm the captain now.", "Abduwali Muse", "Captain Phillips", "Biography, Drama, Thriller"),
    ("I see this as an absolute win!", "Bruce Banner", "Avengers: Endgame", "Action, Adventure, Sci-Fi"),

    # ========== DRAMA / CLASSIC MODERN ==========
    ("Here's the thing about the old days: they the old days.", "Slim Charles", "The Wire", "Crime, Drama"),
    ("Hope is a good thing, maybe the best of things, and no good thing ever dies.", "Andy Dufresne", "The Shawshank Redemption", "Drama"),
    ("Get busy living, or get busy dying.", "Andy Dufresne", "The Shawshank Redemption", "Drama"),
    ("I'm gonna steal the Declaration of Independence.", "Ben Gates", "National Treasure", "Action, Adventure, Mystery"),
    ("We buy things we don't need, with money we don't have, to impress people we don't like.", "Tyler Durden", "Fight Club", "Drama"),
    ("The things you own end up owning you.", "Tyler Durden", "Fight Club", "Drama"),
    ("Why do I fall in love with every woman I see who shows me the least bit of attention?", "Joel Barish", "Eternal Sunshine of the Spotless Mind", "Drama, Romance, Sci-Fi"),
    ("Sell me this pen.", "Jordan Belfort", "The Wolf of Wall Street", "Biography, Comedy, Crime"),
    ("I'm not leaving!", "Jordan Belfort", "The Wolf of Wall Street", "Biography, Comedy, Crime"),
    ("You bow to no one.", "Aragorn", "The Lord of the Rings: The Return of the King", "Adventure, Drama, Fantasy"),
    ("I'm gonna make this pencil disappear.", "The Joker", "The Dark Knight", "Action, Crime, Drama"),

    # ========== PIRATES OF THE CARIBBEAN ==========
    ("But you have heard of me.", "Jack Sparrow", "Pirates of the Caribbean: The Curse of the Black Pearl", "Action, Adventure, Fantasy"),
    ("This is the day you will always remember as the day you almost caught Captain Jack Sparrow.", "Jack Sparrow", "Pirates of the Caribbean: The Curse of the Black Pearl", "Action, Adventure, Fantasy"),
    ("Not all treasure is silver and gold, mate.", "Jack Sparrow", "Pirates of the Caribbean: The Curse of the Black Pearl", "Action, Adventure, Fantasy"),
    ("Why is the rum gone?", "Jack Sparrow", "Pirates of the Caribbean: The Curse of the Black Pearl", "Action, Adventure, Fantasy"),
    ("The problem is not the problem. The problem is your attitude about the problem.", "Jack Sparrow", "Pirates of the Caribbean: The Curse of the Black Pearl", "Action, Adventure, Fantasy"),

    # ========== HORROR ==========
    ("They're all gonna laugh at you!", "Margaret White", "Carrie", "Horror"),
    ("I want to play a game.", "Jigsaw", "Saw", "Horror, Mystery, Thriller"),
    ("Do you like scary movies?", "Ghostface", "Scream", "Horror, Mystery"),
    ("What's your favorite scary movie?", "Ghostface", "Scream", "Horror, Mystery"),
    ("They're heeere.", "Carol Anne Freeling", "Poltergeist", "Horror"),
    ("Be afraid. Be very afraid.", "Veronica Quaife", "The Fly", "Horror, Sci-Fi"),
    ("We all float down here.", "Pennywise", "It", "Horror"),
    ("Hiya, Georgie.", "Pennywise", "It", "Horror"),
    ("Redrum. Redrum.", "Danny Torrance", "The Shining", "Drama, Horror"),

    # ==========================================
    # TV SHOWS
    # ==========================================

    # ========== BREAKING BAD ==========
    ("I am the one who knocks!", "Walter White", "Breaking Bad", "Crime, Drama, Thriller"),
    ("Say my name.", "Walter White", "Breaking Bad", "Crime, Drama, Thriller"),
    ("Tread lightly.", "Walter White", "Breaking Bad", "Crime, Drama, Thriller"),
    ("I am the danger.", "Walter White", "Breaking Bad", "Crime, Drama, Thriller"),
    ("Yeah, science!", "Jesse Pinkman", "Breaking Bad", "Crime, Drama, Thriller"),
    ("This is my own private domicile, and I will not be harassed. B*tch!", "Jesse Pinkman", "Breaking Bad", "Crime, Drama, Thriller"),
    ("You're goddamn right.", "Walter White", "Breaking Bad", "Crime, Drama, Thriller"),
    ("I did it for me. I liked it. I was good at it.", "Walter White", "Breaking Bad", "Crime, Drama, Thriller"),
    ("No half measures.", "Mike Ehrmantraut", "Breaking Bad", "Crime, Drama, Thriller"),
    ("Better call Saul!", "Jesse Pinkman", "Breaking Bad", "Crime, Drama, Thriller"),

    # ========== GAME OF THRONES ==========
    ("Winter is coming.", "Ned Stark", "Game of Thrones", "Action, Adventure, Drama"),
    ("When you play the game of thrones, you win or you die.", "Cersei Lannister", "Game of Thrones", "Action, Adventure, Drama"),
    ("A Lannister always pays his debts.", "Tyrion Lannister", "Game of Thrones", "Action, Adventure, Drama"),
    ("I drink and I know things.", "Tyrion Lannister", "Game of Thrones", "Action, Adventure, Drama"),
    ("You know nothing, Jon Snow.", "Ygritte", "Game of Thrones", "Action, Adventure, Drama"),
    ("Chaos isn't a pit. Chaos is a ladder.", "Petyr Baelish", "Game of Thrones", "Action, Adventure, Drama"),
    ("The night is dark and full of terrors.", "Melisandre", "Game of Thrones", "Action, Adventure, Drama"),
    ("Hold the door!", "Hodor", "Game of Thrones", "Action, Adventure, Drama"),
    ("Dracarys.", "Daenerys Targaryen", "Game of Thrones", "Action, Adventure, Drama"),
    ("Not today.", "Arya Stark", "Game of Thrones", "Action, Adventure, Drama"),
    ("A girl has no name.", "Arya Stark", "Game of Thrones", "Action, Adventure, Drama"),
    ("Valar Morghulis.", "Jaqen H'ghar", "Game of Thrones", "Action, Adventure, Drama"),
    ("The things I do for love.", "Jaime Lannister", "Game of Thrones", "Action, Adventure, Drama"),
    ("Any man who must say 'I am the king' is no true king.", "Tywin Lannister", "Game of Thrones", "Action, Adventure, Drama"),
    ("I choose violence.", "Cersei Lannister", "Game of Thrones", "Action, Adventure, Drama"),
    ("Tell Cersei. I want her to know it was me.", "Olenna Tyrell", "Game of Thrones", "Action, Adventure, Drama"),
    ("The lone wolf dies, but the pack survives.", "Sansa Stark", "Game of Thrones", "Action, Adventure, Drama"),

    # ========== THE OFFICE ==========
    ("That's what she said.", "Michael Scott", "The Office", "Comedy"),
    ("I'm not superstitious, but I am a little stitious.", "Michael Scott", "The Office", "Comedy"),
    ("Bears. Beets. Battlestar Galactica.", "Jim Halpert", "The Office", "Comedy"),
    ("I. Declare. Bankruptcy!", "Michael Scott", "The Office", "Comedy"),
    ("I am Beyonce, always.", "Michael Scott", "The Office", "Comedy"),
    ("Would I rather be feared or loved? Easy. Both. I want people to be afraid of how much they love me.", "Michael Scott", "The Office", "Comedy"),
    ("Identity theft is not a joke, Jim!", "Dwight Schrute", "The Office", "Comedy"),
    ("Sometimes I'll start a sentence and I don't even know where it's going.", "Michael Scott", "The Office", "Comedy"),
    ("I'm gonna need you to come in on Saturday.", "Bill Lumbergh", "Office Space", "Comedy"),
    ("It's Britney, b*tch.", "Michael Scott", "The Office", "Comedy"),
    ("Why are you the way that you are?", "Michael Scott", "The Office", "Comedy"),
    ("Well, well, well, how the turntables...", "Michael Scott", "The Office", "Comedy"),
    ("I feel God in this Chili's tonight.", "Pam Beesly", "The Office", "Comedy"),

    # ========== FRIENDS ==========
    ("We were on a break!", "Ross Geller", "Friends", "Comedy, Romance"),
    ("How you doin'?", "Joey Tribbiani", "Friends", "Comedy, Romance"),
    ("Joey doesn't share food!", "Joey Tribbiani", "Friends", "Comedy, Romance"),
    ("Pivot! Pivot! PIVOT!", "Ross Geller", "Friends", "Comedy, Romance"),
    ("Oh. My. God.", "Janice Litman", "Friends", "Comedy, Romance"),
    ("Could I BE wearing any more clothes?", "Joey Tribbiani", "Friends", "Comedy, Romance"),
    ("He's her lobster.", "Phoebe Buffay", "Friends", "Comedy, Romance"),
    ("I got off the plane.", "Rachel Green", "Friends", "Comedy, Romance"),
    ("It's a moo point. It's like a cow's opinion, it just doesn't matter.", "Joey Tribbiani", "Friends", "Comedy, Romance"),
    ("I KNOW!", "Monica Geller", "Friends", "Comedy, Romance"),
    ("Unagi.", "Ross Geller", "Friends", "Comedy, Romance"),

    # ========== STRANGER THINGS ==========
    ("Friends don't lie.", "Eleven", "Stranger Things", "Drama, Fantasy, Horror"),
    ("I'm the monster.", "Eleven", "Stranger Things", "Drama, Fantasy, Horror"),
    ("She's our friend and she's crazy!", "Dustin Henderson", "Stranger Things", "Drama, Fantasy, Horror"),
    ("Mornings are for coffee and contemplation.", "Chief Hopper", "Stranger Things", "Drama, Fantasy, Horror"),
    ("I dump your ass.", "Eleven", "Stranger Things", "Drama, Fantasy, Horror"),
    ("It's finger-lickin' good. Kentucky Fried Chicken. I'll take you there.", "Eleven", "Stranger Things", "Drama, Fantasy, Horror"),

    # ========== THE MANDALORIAN / STAR WARS TV ==========
    ("This is the way.", "The Mandalorian", "The Mandalorian", "Action, Adventure, Sci-Fi"),
    ("I have spoken.", "Kuiil", "The Mandalorian", "Action, Adventure, Sci-Fi"),
    ("Grogu and I can feel each other's thoughts.", "Din Djarin", "The Mandalorian", "Action, Adventure, Sci-Fi"),

    # ========== RICK AND MORTY ==========
    ("Wubba lubba dub dub!", "Rick Sanchez", "Rick and Morty", "Animation, Adventure, Comedy"),
    ("Nobody exists on purpose. Nobody belongs anywhere. Everybody's gonna die. Come watch TV.", "Morty Smith", "Rick and Morty", "Animation, Adventure, Comedy"),
    ("I'm Pickle Rick!", "Rick Sanchez", "Rick and Morty", "Animation, Adventure, Comedy"),
    ("To live is to risk it all. Otherwise you're just an inert chunk of randomly assembled molecules.", "Rick Sanchez", "Rick and Morty", "Animation, Adventure, Comedy"),
    ("Sometimes science is more art than science.", "Rick Sanchez", "Rick and Morty", "Animation, Adventure, Comedy"),

    # ========== PARKS AND REC ==========
    ("Treat yo self!", "Tom Haverford", "Parks and Recreation", "Comedy"),
    ("I'm a simple man. I like pretty, dark-haired women and breakfast food.", "Ron Swanson", "Parks and Recreation", "Comedy"),
    ("Give me all the bacon and eggs you have.", "Ron Swanson", "Parks and Recreation", "Comedy"),
    ("Everything hurts and I'm dying.", "Leslie Knope", "Parks and Recreation", "Comedy"),
    ("I once worked with a guy for three years and never learned his name.", "Ron Swanson", "Parks and Recreation", "Comedy"),
    ("I know what I'm about, son.", "Ron Swanson", "Parks and Recreation", "Comedy"),

    # ========== SEINFELD ==========
    ("No soup for you!", "The Soup Nazi", "Seinfeld", "Comedy"),
    ("Yada, yada, yada.", "Elaine Benes", "Seinfeld", "Comedy"),
    ("These pretzels are making me thirsty.", "Kramer", "Seinfeld", "Comedy"),
    ("Not that there's anything wrong with that.", "Jerry Seinfeld", "Seinfeld", "Comedy"),
    ("Serenity now!", "Frank Costanza", "Seinfeld", "Comedy"),
    ("I was in the pool!", "George Costanza", "Seinfeld", "Comedy"),

    # ========== THE SIMPSONS ==========
    ("D'oh!", "Homer Simpson", "The Simpsons", "Animation, Comedy"),
    ("Eat my shorts!", "Bart Simpson", "The Simpsons", "Animation, Comedy"),
    ("Don't have a cow, man.", "Bart Simpson", "The Simpsons", "Animation, Comedy"),
    ("Excellent.", "Mr. Burns", "The Simpsons", "Animation, Comedy"),
    ("I, for one, welcome our new insect overlords.", "Kent Brockman", "The Simpsons", "Animation, Comedy"),

    # ========== HOW I MET YOUR MOTHER ==========
    ("It's gonna be legen — wait for it — dary! Legendary!", "Barney Stinson", "How I Met Your Mother", "Comedy, Romance"),
    ("Suit up!", "Barney Stinson", "How I Met Your Mother", "Comedy, Romance"),
    ("Challenge accepted!", "Barney Stinson", "How I Met Your Mother", "Comedy, Romance"),
    ("Nothing good ever happens after 2 a.m.", "Ted Mosby", "How I Met Your Mother", "Comedy, Romance"),
    ("New is always better.", "Barney Stinson", "How I Met Your Mother", "Comedy, Romance"),

    # ========== BROOKLYN NINE-NINE ==========
    ("Cool cool cool cool cool cool cool.", "Jake Peralta", "Brooklyn Nine-Nine", "Comedy, Crime"),
    ("Title of your sex tape.", "Jake Peralta", "Brooklyn Nine-Nine", "Comedy, Crime"),
    ("Nine-nine!", "Jake Peralta", "Brooklyn Nine-Nine", "Comedy, Crime"),
    ("I want it that way.", "Jake Peralta", "Brooklyn Nine-Nine", "Comedy, Crime"),
    ("Bone?! BONE?!", "Captain Holt", "Brooklyn Nine-Nine", "Comedy, Crime"),
    ("Everything is garbage. Never love anything.", "Rosa Diaz", "Brooklyn Nine-Nine", "Comedy, Crime"),

    # ========== IT'S ALWAYS SUNNY ==========
    ("I'm the Trash Man!", "Frank Reynolds", "It's Always Sunny in Philadelphia", "Comedy"),
    ("So anyway, I started blasting.", "Frank Reynolds", "It's Always Sunny in Philadelphia", "Comedy"),
    ("I haven't even begun to peak.", "Dennis Reynolds", "It's Always Sunny in Philadelphia", "Comedy"),
    ("Stupid science b*tch couldn't even make I more smarter.", "Charlie Kelly", "It's Always Sunny in Philadelphia", "Comedy"),
    ("The implication.", "Dennis Reynolds", "It's Always Sunny in Philadelphia", "Comedy"),
    ("Wildcard, b*tches!", "Charlie Kelly", "It's Always Sunny in Philadelphia", "Comedy"),

    # ========== SOUTH PARK ==========
    ("Oh my God, they killed Kenny! You bastards!", "Stan and Kyle", "South Park", "Animation, Comedy"),
    ("Respect my authoritah!", "Eric Cartman", "South Park", "Animation, Comedy"),
    ("I'm not fat, I'm big-boned.", "Eric Cartman", "South Park", "Animation, Comedy"),
    ("Screw you guys, I'm going home.", "Eric Cartman", "South Park", "Animation, Comedy"),

    # ========== PEAKY BLINDERS ==========
    ("By order of the Peaky Blinders!", "Thomas Shelby", "Peaky Blinders", "Crime, Drama"),
    ("I'm not a traitor to my class. I am just an extreme example of what a working man can achieve.", "Thomas Shelby", "Peaky Blinders", "Crime, Drama"),
    ("Everyone's a whore, Grace. We just sell different parts of ourselves.", "Thomas Shelby", "Peaky Blinders", "Crime, Drama"),
    ("I don't pay for suits. My suits are on the house.", "Thomas Shelby", "Peaky Blinders", "Crime, Drama"),

    # ========== OTHER ICONIC TV ==========
    ("Science, b*tch!", "Jesse Pinkman", "Breaking Bad", "Crime, Drama, Thriller"),
    ("You come at the king, you best not miss.", "Omar Little", "The Wire", "Crime, Drama"),
    ("All in the game, yo. All in the game.", "Omar Little", "The Wire", "Crime, Drama"),
    ("You want it to be one way. But it's the other way.", "Marlo Stanfield", "The Wire", "Crime, Drama"),
    ("We're in the empire business.", "Walter White", "Breaking Bad", "Crime, Drama, Thriller"),
    ("Hello, Dexter Morgan.", "Arthur Mitchell", "Dexter", "Crime, Drama, Mystery"),
    ("Surprise, motherf***er.", "James Doakes", "Dexter", "Crime, Drama, Mystery"),
    ("Bazinga!", "Sheldon Cooper", "The Big Bang Theory", "Comedy"),
    ("Clear eyes, full hearts, can't lose.", "Coach Taylor", "Friday Night Lights", "Drama, Sport"),
    ("I've made a huge mistake.", "Gob Bluth", "Arrested Development", "Comedy"),
    ("There's always money in the banana stand.", "George Bluth Sr.", "Arrested Development", "Comedy"),
    ("Troy and Abed in the morning!", "Troy and Abed", "Community", "Comedy"),
    ("Cool. Cool cool cool.", "Abed Nadir", "Community", "Comedy"),
    ("Streets ahead.", "Pierce Hawthorne", "Community", "Comedy"),
    ("I've heard it both ways.", "Shawn Spencer", "Psych", "Comedy, Crime, Mystery"),

    # ========== RECENT / STREAMING ERA ==========
    ("Is Butter a carb?", "Regina George", "Mean Girls", "Comedy"),
    ("Oh hi, Mark.", "Johnny", "The Room", "Drama"),
    ("Are you not entertained?!", "Maximus", "Gladiator", "Action, Adventure, Drama"),
    ("My name is Maximus Decimus Meridius, and I will have my vengeance in this life or the next.", "Maximus", "Gladiator", "Action, Adventure, Drama"),
    ("At my signal, unleash hell.", "Maximus", "Gladiator", "Action, Adventure, Drama"),
    ("Witness me!", "Nux", "Mad Max: Fury Road", "Action, Adventure, Sci-Fi"),
    ("Oh what a day, what a lovely day!", "Nux", "Mad Max: Fury Road", "Action, Adventure, Sci-Fi"),
    ("I live, I die. I live again!", "Nux", "Mad Max: Fury Road", "Action, Adventure, Sci-Fi"),
    ("You mustn't be afraid to dream a little bigger, darling.", "Eames", "Inception", "Action, Adventure, Sci-Fi"),
    ("An idea is like a virus, resilient, highly contagious.", "Cobb", "Inception", "Action, Adventure, Sci-Fi"),
    ("We need to go deeper.", "Cobb", "Inception", "Action, Adventure, Sci-Fi"),
    ("Do you want to build a snowman?", "Anna", "Frozen", "Animation, Adventure, Comedy"),
    ("I volunteer as tribute!", "Katniss Everdeen", "The Hunger Games", "Action, Adventure, Sci-Fi"),
    ("May the odds be ever in your favor.", "Effie Trinket", "The Hunger Games", "Action, Adventure, Sci-Fi"),
    ("If we burn, you burn with us!", "Katniss Everdeen", "The Hunger Games: Mockingjay", "Action, Adventure, Sci-Fi"),
    ("I am no bird; and no net ensnares me.", "Jane Eyre", "Jane Eyre", "Drama, Romance"),
    ("Here's to the ones who dream, foolish as they may seem.", "Mia Dolan", "La La Land", "Comedy, Drama, Music"),
    ("After all, what is a weekend?", "The Dowager Countess", "Downton Abbey", "Drama"),
    ("I'm Mary Poppins, y'all!", "Yondu", "Guardians of the Galaxy Vol. 2", "Action, Adventure, Comedy"),
    ("We are Groot.", "Groot", "Guardians of the Galaxy Vol. 2", "Action, Adventure, Comedy"),
    ("Pew pew!", "Star-Lord", "Guardians of the Galaxy", "Action, Adventure, Comedy"),
    ("That's America's ass.", "Tony Stark", "Avengers: Endgame", "Action, Adventure, Sci-Fi"),
    ("Whatever it takes.", "Steve Rogers", "Avengers: Endgame", "Action, Adventure, Sci-Fi"),
    ("Dormammu, I've come to bargain.", "Doctor Strange", "Doctor Strange", "Action, Adventure, Fantasy"),
    ("No, I don't think I will.", "Steve Rogers", "Avengers: Endgame", "Action, Adventure, Sci-Fi"),
]


# ============================================================
# DATABASE BUILD
# ============================================================

def create_db(conn):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS quotes")
    cursor.execute("""
        CREATE TABLE quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote TEXT NOT NULL,
            character TEXT NOT NULL,
            movie_title TEXT NOT NULL,
            genre TEXT NOT NULL,
            imdb_votes INTEGER NOT NULL DEFAULT 500000,
            difficulty TEXT NOT NULL DEFAULT 'easy',
            year INTEGER DEFAULT NULL,
            is_tv INTEGER NOT NULL DEFAULT 0,
            curated_distractors TEXT DEFAULT NULL
        )
    """)
    cursor.execute("CREATE INDEX idx_genre ON quotes(genre)")
    cursor.execute("CREATE INDEX idx_movie_title ON quotes(movie_title)")
    cursor.execute("CREATE INDEX idx_difficulty ON quotes(difficulty)")
    cursor.execute("CREATE INDEX idx_is_tv ON quotes(is_tv)")
    cursor.execute("CREATE INDEX idx_year ON quotes(year)")
    conn.commit()


def backup_used_quotes(conn):
    """Read existing quotes before rebuild so we can reconcile used IDs."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, quote, movie_title FROM quotes")
        return {row[0]: (row[1].lower().strip(), row[2].lower().strip()) for row in cursor.fetchall()}
    except Exception:
        return {}


def reconcile_used_ids(conn, old_mapping):
    """Map old used_quote_ids to new IDs after rebuild."""
    if not os.path.exists(CONFIG_PATH):
        return
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
    old_used = config.get("global_data", {}).get("used_quote_ids", [])
    if not old_used:
        return

    cursor = conn.cursor()
    new_used = []
    for old_id in old_used:
        old_data = old_mapping.get(old_id)
        if not old_data:
            print(f"  WARNING: Old ID {old_id} not found in backup (was it already missing?)")
            continue
        old_quote, old_title = old_data
        cursor.execute(
            "SELECT id FROM quotes WHERE LOWER(TRIM(quote)) = ? AND LOWER(TRIM(movie_title)) = ?",
            (old_quote, old_title)
        )
        row = cursor.fetchone()
        if row:
            new_used.append(row[0])
            print(f"  Mapped: old ID {old_id} -> new ID {row[0]}")
        else:
            print(f"  WARNING: Used quote (old ID {old_id}) not found in new DB — was it removed?")
            print(f"           Quote: \"{old_quote[:50]}...\" from \"{old_title}\"")

    config["global_data"]["used_quote_ids"] = new_used
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)
    print(f"  Reconciled {len(new_used)}/{len(old_used)} used quote IDs in config")


def main():
    conn = sqlite3.connect(DB_PATH)

    # Back up existing data for ID reconciliation
    old_mapping = backup_used_quotes(conn)

    create_db(conn)
    cursor = conn.cursor()

    seen = set()
    imported = 0
    skipped = 0
    diff_counts = {"easy": 0, "medium": 0, "hard": 0}

    for quote, character, title, genre in QUOTES:
        # Deduplicate by (quote_text, title)
        key = (quote.lower().strip(), title.lower().strip())
        if key in seen:
            skipped += 1
            continue
        seen.add(key)

        # Look up metadata
        year = MOVIE_YEARS.get(title)
        is_tv = 1 if title in TV_SHOWS else 0
        difficulty = DIFFICULTY_OVERRIDES.get((quote, title), "easy")
        curated_key = (quote, title)
        curated = json.dumps(CURATED_DISTRACTORS[curated_key]) if curated_key in CURATED_DISTRACTORS else None

        cursor.execute(
            """INSERT INTO quotes (quote, character, movie_title, genre, difficulty, year, is_tv, curated_distractors)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (quote.strip(), character.strip(), title.strip(), genre.strip(),
             difficulty, year, is_tv, curated)
        )
        imported += 1
        diff_counts[difficulty] += 1

    conn.commit()

    # Stats
    cursor.execute("SELECT COUNT(*) FROM quotes")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT movie_title) FROM quotes")
    titles = cursor.fetchone()[0]

    tv_count = sum(1 for q, c, t, g in QUOTES if t in TV_SHOWS and (q.lower().strip(), t.lower().strip()) in seen)
    movie_count = total - diff_counts.get("_tv", 0)  # rough

    print(f"{'=' * 60}")
    print(f"  Curated quotes database built!")
    print(f"{'=' * 60}")
    print(f"  Total quotes:       {total}")
    print(f"  Duplicates removed: {skipped}")
    print(f"  Unique titles:      {titles}")
    print()
    print(f"  Difficulty breakdown:")
    print(f"    Easy:   {diff_counts['easy']:>3} ({diff_counts['easy']/total*100:.0f}%)")
    print(f"    Medium: {diff_counts['medium']:>3} ({diff_counts['medium']/total*100:.0f}%)")
    print(f"    Hard:   {diff_counts['hard']:>3} ({diff_counts['hard']/total*100:.0f}%)")
    print()
    print(f"  Curated distractors: {sum(1 for q,c,t,g in QUOTES if (q,t) in CURATED_DISTRACTORS)}")
    print()

    # Year coverage check
    cursor.execute("SELECT COUNT(*) FROM quotes WHERE year IS NULL")
    missing_years = cursor.fetchone()[0]
    if missing_years:
        cursor.execute("SELECT DISTINCT movie_title FROM quotes WHERE year IS NULL")
        missing = [r[0] for r in cursor.fetchall()]
        print(f"  ⚠ {missing_years} quotes missing year data:")
        for t in missing:
            print(f"    - {t}")
        print()

    # Reconcile used IDs
    if old_mapping:
        print("  Reconciling used quote IDs...")
        reconcile_used_ids(conn, old_mapping)
    else:
        print("  No old database found — skipping ID reconciliation")

    print()
    print("  Quotes per title (top 15):")
    cursor.execute("SELECT movie_title, COUNT(*) as cnt FROM quotes GROUP BY movie_title ORDER BY cnt DESC LIMIT 15")
    for t, cnt in cursor.fetchall():
        bar = "█" * cnt
        print(f"    {cnt:>2} {bar} {t}")

    conn.close()
    print(f"\n  Database saved to: {DB_PATH}")


if __name__ == "__main__":
    main()
