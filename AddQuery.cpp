#include "AddQuery.h"
#include "../../db/Database.h"

constexpr const char *AddQuery::qname;

QueryResult::Ptr AddQuery::execute()
{
    using namespace std;
    if (this->operands.size() < 2)
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
            for (auto it : table)
            {
                if (this->evalCondition(it))
                {
                    auto temp = it[this->operands[0]];
                    for (size_t i = 1; i < this->operands.size(); ++i)
                        temp += it[this->operands[i]];
                    it[this->operands[operands.size() - 1]] = temp;
                    ++cnt;
                }
            }
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

std::string AddQuery::toString()
{
    return "QUERY = ADD " + this->targetTable + "\"";
}
