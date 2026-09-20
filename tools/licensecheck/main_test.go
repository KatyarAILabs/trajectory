// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

func TestIdentify(t *testing.T) {
	cases := []struct {
		name, text, want string
	}{
		{"apache", "Apache License\nVersion 2.0, January 2004", "Apache-2.0"},
		{"mit", "Permission is hereby granted, free of charge, to any person", "MIT"},
		{"isc", "Permission to use, copy, modify, and/or distribute this software", "ISC"},
		{"mpl", "Mozilla Public License Version 2.0", "MPL-2.0"},
		{"unlicense", "This is free and unencumbered software released into the public domain", "Unlicense"},

		{
			"bsd3",
			"Redistribution and use in source and binary forms, with or without modification, " +
				"are permitted provided that the following conditions are met: " +
				"Neither the name of the copyright holder nor the names of its contributors",
			"BSD-3-Clause",
		},
		{
			"bsd2",
			"Redistribution and use in source and binary forms, with or without modification, " +
				"are permitted provided that the following conditions are met: " +
				"1. Redistributions of source code must retain the above copyright notice.",
			"BSD-2-Clause",
		},

		// Copyleft must be identified, not merely left unrecognised, so
		// the failure message names what it actually is.
		{"gpl", "GNU GENERAL PUBLIC LICENSE Version 3, 29 June 2007", "GPL"},
		{"lgpl", "GNU LESSER GENERAL PUBLIC LICENSE Version 2.1", "LGPL"},
		{"agpl", "GNU AFFERO GENERAL PUBLIC LICENSE Version 3", "AGPL"},

		// Fails closed: text this tool cannot place is not a pass.
		{"unknown", "You may use this software if you send the author a postcard.", ""},
		{"empty", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := identify(tc.text); got != tc.want {
				t.Errorf("identify() = %q, want %q", got, tc.want)
			}
		})
	}
}

// Line wrapping and indentation vary between projects shipping the same
// licence, so whitespace must not change the verdict.
func TestIdentifyIsWhitespaceInsensitive(t *testing.T) {
	wrapped := "Apache\n   License\n\n\tVersion 2.0,\nJanuary 2004"
	if got := identify(wrapped); got != "Apache-2.0" {
		t.Errorf("identify(wrapped) = %q, want Apache-2.0", got)
	}
}

// A copyleft licence that also quotes permissive boilerplate must still be
// rejected; the copyleft check has to win.
func TestCopyleftWinsOverPermissivePhrases(t *testing.T) {
	mixed := "GNU GENERAL PUBLIC LICENSE Version 2. " +
		"Permission is hereby granted, free of charge, to any person obtaining a copy"
	if got := identify(mixed); got != "GPL" {
		t.Errorf("identify(mixed) = %q, want GPL", got)
	}
}

// Every licence this tool can name is either on the allow-list or is one it
// names in order to reject. A licence that is recognised but neither allowed
// nor copyleft would be silently... allowed, which is the bug this catches.
func TestAllowListCoversRecognisedPermissiveLicences(t *testing.T) {
	for _, l := range []string{"Apache-2.0", "MIT", "BSD-2-Clause", "BSD-3-Clause", "ISC", "MPL-2.0", "Unlicense", "Zlib"} {
		if !allowed[l] {
			t.Errorf("%s is recognised by identify() but missing from the allow-list", l)
		}
	}
	for _, l := range []string{"GPL", "LGPL", "AGPL"} {
		if allowed[l] {
			t.Errorf("%s must not be on the allow-list", l)
		}
	}
}
