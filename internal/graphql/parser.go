package graphql

import (
	"fmt"
	"strconv"
	"strings"
)

// AST types for a subset-complete GraphQL v4 recursive-descent parser.

type Document struct {
	Operations []*OperationDef
	Fragments  map[string]*FragmentDef
}

type OperationDef struct {
	Kind         string // "query" | "mutation"
	Name         string
	SelectionSet []Selection
}

type FragmentDef struct {
	Name         string
	TypeCond     string
	SelectionSet []Selection
}

type Selection struct {
	Kind         string // "field" | "inline_fragment" | "fragment_spread"
	Alias        string
	Name         string
	TypeCond     string // for inline_fragment
	Args         map[string]ValueNode
	SelectionSet []Selection
}

func (s Selection) ResponseKey() string {
	if s.Alias != "" {
		return s.Alias
	}
	return s.Name
}

type ValueKind uint8

const (
	ValVariable ValueKind = iota + 1
	ValInt
	ValFloat
	ValString
	ValBool
	ValNull
	ValEnum
	ValList
	ValObject
)

type ValueNode struct {
	Kind   ValueKind
	Str    string
	Int    int64
	Bool   bool
	List   []ValueNode
	Object map[string]ValueNode
}

type tokenKind uint8

const (
	tokEOF tokenKind = iota
	tokName
	tokInt
	tokString
	tokBang    // !
	tokDollar  // $
	tokLParen  // (
	tokRParen  // )
	tokSpread  // ...
	tokColon   // :
	tokEquals  // =
	tokAt      // @
	tokLBrack  // [
	tokRBrack  // ]
	tokLBrace  // {
	tokRBrace  // }
)

type token struct {
	kind tokenKind
	val  string
}

type parser struct {
	src string
	pos int
	cur token
}

func ParseDocument(query string) (*Document, error) {
	p := &parser{src: query}
	if err := p.advance(); err != nil {
		return nil, err
	}
	doc := &Document{
		Fragments: make(map[string]*FragmentDef),
	}
	for p.cur.kind != tokEOF {
		if p.cur.kind == tokLBrace {
			sels, err := p.parseSelectionSet()
			if err != nil {
				return nil, err
			}
			doc.Operations = append(doc.Operations, &OperationDef{
				Kind:         "query",
				SelectionSet: sels,
			})
			continue
		}
		if p.cur.kind != tokName {
			return nil, fmt.Errorf("unexpected token %q in document", p.cur.val)
		}
		switch p.cur.val {
		case "query", "mutation", "subscription":
			opKind := p.cur.val
			if err := p.advance(); err != nil {
				return nil, err
			}
			var opName string
			if p.cur.kind == tokName {
				opName = p.cur.val
				if err := p.advance(); err != nil {
					return nil, err
				}
			}
			if p.cur.kind == tokLParen {
				if err := p.skipVarDefs(); err != nil {
					return nil, err
				}
			}
			if err := p.skipDirectives(); err != nil {
				return nil, err
			}
			sels, err := p.parseSelectionSet()
			if err != nil {
				return nil, err
			}
			doc.Operations = append(doc.Operations, &OperationDef{
				Kind:         opKind,
				Name:         opName,
				SelectionSet: sels,
			})

		case "fragment":
			if err := p.advance(); err != nil {
				return nil, err
			}
			if p.cur.kind != tokName {
				return nil, fmt.Errorf("expected fragment name")
			}
			fragName := p.cur.val
			if err := p.advance(); err != nil {
				return nil, err
			}
			if p.cur.kind != tokName || p.cur.val != "on" {
				return nil, fmt.Errorf("expected 'on' in fragment definition")
			}
			if err := p.advance(); err != nil {
				return nil, err
			}
			if p.cur.kind != tokName {
				return nil, fmt.Errorf("expected type condition name")
			}
			typeCond := p.cur.val
			if err := p.advance(); err != nil {
				return nil, err
			}
			if err := p.skipDirectives(); err != nil {
				return nil, err
			}
			sels, err := p.parseSelectionSet()
			if err != nil {
				return nil, err
			}
			doc.Fragments[fragName] = &FragmentDef{
				Name:         fragName,
				TypeCond:     typeCond,
				SelectionSet: sels,
			}

		default:
			return nil, fmt.Errorf("unexpected top-level keyword %q", p.cur.val)
		}
	}
	if len(doc.Operations) == 0 {
		return nil, fmt.Errorf("document contains no executable operation")
	}
	return doc, nil
}

func (p *parser) skipVarDefs() error {
	if err := p.expect(tokLParen); err != nil {
		return err
	}
	depth := 1
	for depth > 0 && p.cur.kind != tokEOF {
		if p.cur.kind == tokLParen {
			depth++
		} else if p.cur.kind == tokRParen {
			depth--
			if depth == 0 {
				return p.advance()
			}
		}
		if err := p.advance(); err != nil {
			return err
		}
	}
	return fmt.Errorf("unterminated variable definitions")
}

func (p *parser) skipDirectives() error {
	for p.cur.kind == tokAt {
		if err := p.advance(); err != nil {
			return err
		}
		if p.cur.kind != tokName {
			return fmt.Errorf("expected directive name after @")
		}
		if err := p.advance(); err != nil {
			return err
		}
		if p.cur.kind == tokLParen {
			if _, err := p.parseArguments(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *parser) parseSelectionSet() ([]Selection, error) {
	if err := p.expect(tokLBrace); err != nil {
		return nil, err
	}
	var out []Selection
	for p.cur.kind != tokRBrace && p.cur.kind != tokEOF {
		if p.cur.kind == tokSpread {
			if err := p.advance(); err != nil {
				return nil, err
			}
			if p.cur.kind == tokName && p.cur.val == "on" {
				if err := p.advance(); err != nil {
					return nil, err
				}
				if p.cur.kind != tokName {
					return nil, fmt.Errorf("expected type name after '... on'")
				}
				tc := p.cur.val
				if err := p.advance(); err != nil {
					return nil, err
				}
				if err := p.skipDirectives(); err != nil {
					return nil, err
				}
				sub, err := p.parseSelectionSet()
				if err != nil {
					return nil, err
				}
				out = append(out, Selection{
					Kind:         "inline_fragment",
					TypeCond:     tc,
					SelectionSet: sub,
				})
				continue
			}
			if p.cur.kind == tokLBrace {
				sub, err := p.parseSelectionSet()
				if err != nil {
					return nil, err
				}
				out = append(out, Selection{
					Kind:         "inline_fragment",
					SelectionSet: sub,
				})
				continue
			}
			if p.cur.kind != tokName {
				return nil, fmt.Errorf("expected fragment name after '...'")
			}
			fragName := p.cur.val
			if err := p.advance(); err != nil {
				return nil, err
			}
			if err := p.skipDirectives(); err != nil {
				return nil, err
			}
			out = append(out, Selection{
				Kind: "fragment_spread",
				Name: fragName,
			})
			continue
		}

		if p.cur.kind != tokName {
			return nil, fmt.Errorf("expected field name, got %q", p.cur.val)
		}
		first := p.cur.val
		if err := p.advance(); err != nil {
			return nil, err
		}
		var alias, name string
		if p.cur.kind == tokColon {
			if err := p.advance(); err != nil {
				return nil, err
			}
			if p.cur.kind != tokName {
				return nil, fmt.Errorf("expected field name after alias colon")
			}
			alias = first
			name = p.cur.val
			if err := p.advance(); err != nil {
				return nil, err
			}
		} else {
			name = first
		}

		var args map[string]ValueNode
		if p.cur.kind == tokLParen {
			var err error
			args, err = p.parseArguments()
			if err != nil {
				return nil, err
			}
		}
		if err := p.skipDirectives(); err != nil {
			return nil, err
		}
		var sub []Selection
		if p.cur.kind == tokLBrace {
			var err error
			sub, err = p.parseSelectionSet()
			if err != nil {
				return nil, err
			}
		}
		out = append(out, Selection{
			Kind:         "field",
			Alias:        alias,
			Name:         name,
			Args:         args,
			SelectionSet: sub,
		})
	}
	if err := p.expect(tokRBrace); err != nil {
		return nil, err
	}
	return out, nil
}

func (p *parser) parseArguments() (map[string]ValueNode, error) {
	if err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	args := make(map[string]ValueNode, 4)
	for p.cur.kind != tokRParen && p.cur.kind != tokEOF {
		if p.cur.kind != tokName {
			return nil, fmt.Errorf("expected argument name")
		}
		argName := p.cur.val
		if err := p.advance(); err != nil {
			return nil, err
		}
		if err := p.expect(tokColon); err != nil {
			return nil, err
		}
		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		args[argName] = val
	}
	if err := p.expect(tokRParen); err != nil {
		return nil, err
	}
	return args, nil
}

func (p *parser) parseValue() (ValueNode, error) {
	switch p.cur.kind {
	case tokDollar:
		if err := p.advance(); err != nil {
			return ValueNode{}, err
		}
		if p.cur.kind != tokName {
			return ValueNode{}, fmt.Errorf("expected variable name after $")
		}
		v := p.cur.val
		if err := p.advance(); err != nil {
			return ValueNode{}, err
		}
		return ValueNode{Kind: ValVariable, Str: v}, nil

	case tokInt:
		raw := p.cur.val
		if err := p.advance(); err != nil {
			return ValueNode{}, err
		}
		if strings.ContainsAny(raw, ".eE") {
			return ValueNode{Kind: ValFloat, Str: raw}, nil
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return ValueNode{}, err
		}
		return ValueNode{Kind: ValInt, Int: n}, nil

	case tokString:
		s := p.cur.val
		if err := p.advance(); err != nil {
			return ValueNode{}, err
		}
		return ValueNode{Kind: ValString, Str: s}, nil

	case tokName:
		w := p.cur.val
		if err := p.advance(); err != nil {
			return ValueNode{}, err
		}
		switch w {
		case "true":
			return ValueNode{Kind: ValBool, Bool: true}, nil
		case "false":
			return ValueNode{Kind: ValBool, Bool: false}, nil
		case "null":
			return ValueNode{Kind: ValNull}, nil
		default:
			return ValueNode{Kind: ValEnum, Str: w}, nil
		}

	case tokLBrack:
		if err := p.advance(); err != nil {
			return ValueNode{}, err
		}
		var list []ValueNode
		for p.cur.kind != tokRBrack && p.cur.kind != tokEOF {
			elem, err := p.parseValue()
			if err != nil {
				return ValueNode{}, err
			}
			list = append(list, elem)
		}
		if err := p.expect(tokRBrack); err != nil {
			return ValueNode{}, err
		}
		return ValueNode{Kind: ValList, List: list}, nil

	case tokLBrace:
		if err := p.advance(); err != nil {
			return ValueNode{}, err
		}
		obj := make(map[string]ValueNode)
		for p.cur.kind != tokRBrace && p.cur.kind != tokEOF {
			if p.cur.kind != tokName {
				return ValueNode{}, fmt.Errorf("expected object field name")
			}
			k := p.cur.val
			if err := p.advance(); err != nil {
				return ValueNode{}, err
			}
			if err := p.expect(tokColon); err != nil {
				return ValueNode{}, err
			}
			v, err := p.parseValue()
			if err != nil {
				return ValueNode{}, err
			}
			obj[k] = v
		}
		if err := p.expect(tokRBrace); err != nil {
			return ValueNode{}, err
		}
		return ValueNode{Kind: ValObject, Object: obj}, nil

	default:
		return ValueNode{}, fmt.Errorf("unexpected value token %q", p.cur.val)
	}
}

func (p *parser) expect(k tokenKind) error {
	if p.cur.kind != k {
		return fmt.Errorf("unexpected token %q", p.cur.val)
	}
	return p.advance()
}

func (p *parser) advance() error {
	s := p.src
	n := len(s)
	for p.pos < n {
		c := s[p.pos]
		// Ignored tokens in GraphQL: whitespace, commas, BOM, and # comments
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ',' {
			p.pos++
			continue
		}
		if c == '#' {
			for p.pos < n && s[p.pos] != '\n' && s[p.pos] != '\r' {
				p.pos++
			}
			continue
		}
		break
	}
	if p.pos >= n {
		p.cur = token{kind: tokEOF}
		return nil
	}
	c := s[p.pos]
	switch c {
	case '!':
		p.pos++
		p.cur = token{kind: tokBang, val: "!"}
		return nil
	case '$':
		p.pos++
		p.cur = token{kind: tokDollar, val: "$"}
		return nil
	case '(':
		p.pos++
		p.cur = token{kind: tokLParen, val: "("}
		return nil
	case ')':
		p.pos++
		p.cur = token{kind: tokRParen, val: ")"}
		return nil
	case ':':
		p.pos++
		p.cur = token{kind: tokColon, val: ":"}
		return nil
	case '=':
		p.pos++
		p.cur = token{kind: tokEquals, val: "="}
		return nil
	case '@':
		p.pos++
		p.cur = token{kind: tokAt, val: "@"}
		return nil
	case '[':
		p.pos++
		p.cur = token{kind: tokLBrack, val: "["}
		return nil
	case ']':
		p.pos++
		p.cur = token{kind: tokRBrack, val: "]"}
		return nil
	case '{':
		p.pos++
		p.cur = token{kind: tokLBrace, val: "{"}
		return nil
	case '}':
		p.pos++
		p.cur = token{kind: tokRBrace, val: "}"}
		return nil
	case '.':
		if p.pos+2 < n && s[p.pos+1] == '.' && s[p.pos+2] == '.' {
			p.pos += 3
			p.cur = token{kind: tokSpread, val: "..."}
			return nil
		}
		return fmt.Errorf("unexpected '.' at byte %d", p.pos)
	case '"':
		str, err := p.lexString()
		if err != nil {
			return err
		}
		p.cur = token{kind: tokString, val: str}
		return nil
	}
	if c == '-' || (c >= '0' && c <= '9') {
		start := p.pos
		if c == '-' {
			p.pos++
		}
		for p.pos < n && ((s[p.pos] >= '0' && s[p.pos] <= '9') || s[p.pos] == '.' || s[p.pos] == 'e' || s[p.pos] == 'E' || s[p.pos] == '+' || s[p.pos] == '-') {
			p.pos++
		}
		p.cur = token{kind: tokInt, val: s[start:p.pos]}
		return nil
	}
	if isNameStart(c) {
		start := p.pos
		p.pos++
		for p.pos < n && isNameCont(s[p.pos]) {
			p.pos++
		}
		p.cur = token{kind: tokName, val: s[start:p.pos]}
		return nil
	}
	return fmt.Errorf("unexpected character %q at byte %d", c, p.pos)
}

func isNameStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isNameCont(c byte) bool {
	return isNameStart(c) || (c >= '0' && c <= '9')
}

func (p *parser) lexString() (string, error) {
	s := p.src
	n := len(s)
	// Block string """..."""
	if p.pos+2 < n && s[p.pos+1] == '"' && s[p.pos+2] == '"' {
		p.pos += 3
		start := p.pos
		for p.pos+2 < n {
			if s[p.pos] == '"' && s[p.pos+1] == '"' && s[p.pos+2] == '"' {
				val := s[start:p.pos]
				p.pos += 3
				return val, nil
			}
			p.pos++
		}
		return "", fmt.Errorf("unterminated block string")
	}

	p.pos++ // skip opening '"'
	var b strings.Builder
	for p.pos < n {
		c := s[p.pos]
		if c == '"' {
			p.pos++
			return b.String(), nil
		}
		if c == '\\' {
			p.pos++
			if p.pos >= n {
				return "", fmt.Errorf("unterminated string escape")
			}
			esc := s[p.pos]
			p.pos++
			switch esc {
			case '"', '\\', '/':
				b.WriteByte(esc)
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'u':
				if p.pos+4 > n {
					return "", fmt.Errorf("truncated unicode escape")
				}
				code, err := strconv.ParseUint(s[p.pos:p.pos+4], 16, 32)
				if err != nil {
					return "", err
				}
				p.pos += 4
				b.WriteRune(rune(code))
			default:
				b.WriteByte(esc)
			}
			continue
		}
		b.WriteByte(c)
		p.pos++
	}
	return "", fmt.Errorf("unterminated string literal")
}
