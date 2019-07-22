sed 's/\t/,/g; s/,\(.*\)/,\1,\1/g; 1i\class,id,label' points > points.csv
sed 's/[<>]*//g; s/\t/,/g; 1i\Source,Target,Weight' edges > edges.csv
