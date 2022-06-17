package protodb

import "regexp"

var (
	regexSelColumnAlias = regexp.MustCompile(`(?i)^.+ AS ([a-zA-Z]|[0-9]|_)+$`)
	regexField          = regexp.MustCompile(`\$(([a-zA-Z]|[0-9]|_)+)`)
)
