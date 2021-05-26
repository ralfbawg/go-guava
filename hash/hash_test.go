package hash

import "testing"

func TestFnvHash(t *testing.T) {
	tests := []struct {
		key  string
		want uint32
	}{
		{"300000000$AjVrqYAUmUqg", 604783963},
		{"300000000$DryrKaDqnYvA", 1079278295},
		{"300000000$XZluUZLcovWt", 986544959},
		{"300000000$VuMRoDdTTpKn", 1547744574},
		{"300000000$EdYEUlNmVEVf", 1843390748},
		{"300000000$QiGfEKXUrrYP", 1803405908},
		{"300000000$RAJVbSjGmpsE", 1590726411},
		{"300000000$XOKqQfPuXFHx", 1885975492},
		{"300000000$GIBeWKURGtBr", 1629192277},
		{"144115215606350110$p5e1bcaee006e74af940445dc085d3941", 1202241285},
		{"144115215606350110$p5e1bcaee006e728ce5b440a21994adce", 55042684},
		{"300000000$fvpXwlrneoGW", 203694727},
		{"300000000$fOayAIggnSRU", 681812577},
		{"300000000$XztIqtnaUMdH", 845797753},
		{"300000000$cITcqGDYkgEe", 1676158787},
		{"300000000$VsjizxWNbegv", 1761272572},
		{"300000000$JiIKeCgpBlEL", 443614287},
		{"144115215606350143$p5e1bcaee008f74af94046e340add0ad7", 471254813},
		{"300000000$HtffbwsdFHzb", 108675372},
		{"300000000$JIHKrEgzNWhL", 1050667928},
		{"300000000$XnfNnNzmnimd", 590582090},
		{"300000000$OkNtjAfSWCsw", 1874819521},
		{"300000000$UzLHWJVsraNQ", 1638541024},
		{"300000000$AWKzTgBqKwRG", 857538369},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if got := FnvHash([]byte(tt.key)); got != tt.want {
				t.Errorf("FnvHash() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMurmurHash(t *testing.T) {
	tests := []struct {
		key  string
		want uint32
	}{
		{"300000000$AjVrqYAUmUqg", 939990846},
		{"300000000$DryrKaDqnYvA", 2912835585},
		{"300000000$XZluUZLcovWt", 4100439855},
		{"300000000$VuMRoDdTTpKn", 803615380},
		{"300000000$EdYEUlNmVEVf", 3692244050},
		{"300000000$QiGfEKXUrrYP", 453637274},
		{"300000000$RAJVbSjGmpsE", 2125143308},
		{"300000000$XOKqQfPuXFHx", 1988700385},
		{"300000000$GIBeWKURGtBr", 819916990},
		{"144115215606350110$p5e1bcaee006e74af940445dc085d3941", 2586119480},
		{"144115215606350110$p5e1bcaee006e728ce5b440a21994adce", 1949251667},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if got := MurmurHash2(tt.key); got != tt.want {
				t.Errorf("MurmurHash() = %v, want %v", got, tt.want)
			}
		})
	}
}
