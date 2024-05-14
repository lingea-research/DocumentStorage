CREATE TABLE Sentence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT UNIQUE
);

CREATE TABLE Paragraph (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT UNIQUE
);

CREATE TABLE Document (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT UNIQUE
);

CREATE TABLE ParagraphOfSentence (
    paragraph INTEGER,
    sentence INTEGER,
    position INTEGER,
	FOREIGN KEY (paragraph) REFERENCES Paragraph (id) ON DELETE CASCADE ,
	FOREIGN KEY (sentence) REFERENCES Sentence (id) ON DELETE CASCADE
);

CREATE TABLE Url (
    id INTEGER PRIMARY KEY,
    url TEXT UNIQUE
);

CREATE TABLE DocumentOfParagraph (
    document INTEGER,
    paragraph INTEGER,
    position INTEGER,
	FOREIGN KEY (document) REFERENCES Document (id) ON DELETE CASCADE,
	FOREIGN KEY (paragraph) REFERENCES Sentence (id) ON DELETE CASCADE
);

CREATE TABLE DocumentsPart (
    part INTEGER
    aggregate INTEGER
);

CREATE TABLE Occurrence (
    document INTEGER,
    url INTEGER,
    time INTEGER,
    indexerId TEXT,
    FOREIGN KEY (document) REFERENCES Document (id) ON DELETE CASCADE,
    FOREIGN KEY (url) REFERENCES url (id) ON DELETE CASCADE
);

CREATE TABLE SentenceOccurrence (
    sentence INTEGER,
    document INTEGER,
    paragraph INTEGER,
    documentPos INTEGER,
    paragraphPos INTEGER,
    FOREIGN KEY (sentence) REFERENCES Sentence (id) ON DELETE CASCADE,
    FOREIGN KEY (document) REFERENCES Document (id) ON DELETE CASCADE,
    FOREIGN KEY (paragraph) REFERENCES Paragraph (id) ON DELETE CASCADE
);
