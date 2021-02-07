
import json,csv
import sys
output=sys.argv[2]

with open("s3://dagtest/asset/asset.json") as f:
    data=json.load(f)

names=["country",
"description",
"drm",
"episodeCount",
"genre",
"imageURL",
"language",
"seasonSlug",
"title",
"slug",
"tvChannel"  ]




with open(output,"w",newline="") as file:
    csv_file=csv.writer(file)
    csv_file.writerow(names)
    for item in data["contents"]:
        csv_file.writerow([ item["country"],
            item["description"],
            item["drm"],
            item["episodeCount"],
            item["genre"],
            item["imageURL"],
            item["language"],
            item["seasonSlug"],
            item["title"],
            item["slug"],
            item["tvChannel"]  ])

