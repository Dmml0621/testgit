#include "DuplicateQuery.h"
#include "../../db/Database.h"

constexpr const char *DuplicateQuery::qname;

QueryResult::Ptr DuplicateQuery::execute()
{
    using namespace std;
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    Table::SizeType cnt = 0;
    try{
        auto &table = db[this->targetTable];
        auto res = initCondition(table);
        if (res.second)
        {
            vector<vector<Table::ValueType> > vdata;
            vector<Table::KeyType> vkey;
            auto it = table.begin();
            for (size_t i = 0; i < table.size(); ++i)
            {
                if (this->evalCondition(*it))
                {
                    auto newkey = it->key() + "_copy";
                    if (table[newkey] != nullptr)
                    {
                        it++;
                        continue;
                    }
                    vector<Table::ValueType> temp;
                    temp.clear();
                    for (size_t j = 0; j < table.field().size(); ++j)
                        temp.emplace_back((*it)[j]);
                    vdata.emplace_back(temp);
                    vkey.emplace_back(newkey);
                    ++cnt;
                }
                ++it;
            }
            for (size_t j=0; j < vkey.size(); ++j)
                table.insertByIndex(vkey[j], move(vdata[j]));
        }
        return make_unique<RecordCountResult>(cnt);
    }
    catch (const TableNameNotFound &e){
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
    }
}

std::string DuplicateQuery::toString()
{
    return "QUERY = Duplicate " + this->targetTable + "\"";
}
