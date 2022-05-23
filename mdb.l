%option noyywrap
%option nounput
%{
#include "lexer.h"
%}
COLON         [\:]
L_ARROW       "<-"
R_ARROW       "->"
IMPLICIT      [@]+
TRUE          "true"
FALSE         "false"
STRING        \"([^"\\]|\\.)*\"
IDENTIFIER    [a-zA-Z][a-zA-Z0-9_]*
IRI           <([^><\"{}\^\\\|`\x00-\x20])*>
ANON          "_a"[1-9][0-9]*
INTEGER       [-+]?[0-9]+
FLOAT         [-+]?[0-9]*\.[0-9]+
WHITESPACE    [ \r\t]+
ENDLINE       [\n<<EOF>>]
UNRECOGNIZED  .
%%
{COLON}         { return Token::COLON; }
{L_ARROW}       { return Token::L_ARROW; }
{R_ARROW}       { return Token::R_ARROW; }
{IMPLICIT}      { return Token::IMPLICIT; }
{TRUE}          { return Token::TRUE; }
{FALSE}         { return Token::FALSE; }
{STRING}        { return Token::STRING; }
{IDENTIFIER}    { return Token::IDENTIFIER; }
{IRI}           { return Token::IRI; }
{ANON}          { return Token::ANON; }
{INTEGER}       { return Token::INTEGER; }
{FLOAT}         { return Token::FLOAT; }
{WHITESPACE}    { return Token::WHITESPACE; }
{ENDLINE}       { return Token::ENDLINE; }
{UNRECOGNIZED}  { return Token::UNRECOGNIZED; }
%%

void Lexer::begin() {
    yyin = stdin;
}

int Lexer::next_token() {
    int res = yylex();
    str = yytext;
    str_len = yyleng;
    return res;
}
