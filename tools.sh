MODULE_DIR=core/modules/proto
grep -rwn shared_ptr core| grep -w new | grep "${MODULE_DIR}" &&
cd "${MODULE_DIR}" &&
find . -name "*.cc" -o -name "*.h" | xargs perl -i  -pe 's/boost::shared_ptr<([a-zA-Z:]*)> ([a-zA-Z]+)\(new ([a-zA-Z:]*)(\(.*\))\)/boost::shared_ptr<\1> \2 = boost::make_shared<\3>\4/g' &&
find . -name "*.cc" -o -name "*.h" | xargs perl -i  -pe 's/boost::shared_ptr<([a-zA-Z:]*)> ([a-zA-Z]+)\(new ([a-zA-Z:]*)(\(\))\)/boost::shared_ptr<\1> \2 = boost::make_shared<\3>()/g' &&
cd - &&
printf "\nAFTER\n\n"
grep -rwn shared_ptr core| grep -w new | grep "${MODULE_DIR}"

# remains
# grep -rwn shared_ptr core| grep -v parser| grep -v css| grep -v query| grep -v wbase| grep -v wcontrol | grep -w new
