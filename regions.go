package cloudtasks

import "fmt"

func regionCode(region string) string {
	switch region {
	case "europe-west1":
		return "ew"
	default:
		panic(fmt.Sprintf("cloudtasks: missing region %q in the library list of suffixes, open a PR to add it", region))
	}
}
