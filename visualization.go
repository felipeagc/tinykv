package tinykv

import (
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func visualizeRoot(p page) error {
	var sb strings.Builder
	sb.WriteString("digraph G { rank=same; rankdir=\"LR\"; \n")
	visualizePage(p, &sb)
	sb.WriteString("}\n")

	err := os.WriteFile("/tmp/db.dot", []byte(sb.String()), 0600)
	if err != nil {
		return err
	}

	err = exec.Command("dot", "-Tpdf", "/tmp/db.dot", "-o", "/tmp/db.pdf").Run()
	if err != nil {
		return err
	}

	err = exec.Command("xdg-open", "/tmp/db.pdf").Run()
	if err != nil {
		return err
	}

	return nil
}

func visualizePage(p page, sb *strings.Builder) {
	switch p.(type) {
	case *leafPage:
		leaf := p.(*leafPage)
		usedBytes := pageSize - leaf.getFreeSpace()
		label := fmt.Sprintf("Page %d (%d/%d bytes used)", leaf.getIndex(), usedBytes, pageSize)

		sb.WriteString(fmt.Sprintf(`	subgraph cluster_p%d {
		style=filled;
		color=lightgrey;
		node [style=filled,color=white];
		label = "%s";
`, leaf.getIndex(), label))

		lastNode := ""
		leaf.iterCells(func(key, value []byte, offset uint32) bool {
			keyName := "n" + hex.EncodeToString(key)
			sb.WriteString(fmt.Sprintf(
				"		%s [label=\"%s = %s\\noffset = %d\"];\n",
				keyName,
				string(key),
				string(value),
				offset,
			))
			if lastNode != "" {
				sb.WriteString(fmt.Sprintf("		%s -> %s;\n", lastNode, keyName))
			}
			lastNode = keyName
			return true
		})

		sb.WriteString("	}\n")
	}
}
