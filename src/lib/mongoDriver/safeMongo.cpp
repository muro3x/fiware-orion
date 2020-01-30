/*
*
* Copyright 2020 Telefonica Investigacion y Desarrollo, S.A.U
*
* This file is part of Orion Context Broker.
*
* Orion Context Broker is free software: you can redistribute it and/or
* modify it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* Orion Context Broker is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero
* General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with Orion Context Broker. If not, see http://www.gnu.org/licenses/.
*
* For those usages not covered by this license please contact with
* iot_support at tid dot es
*
* Author: Fermin Galan Marquez
*/
#include <string>
#include <vector>

#include "logMsg/logMsg.h"
#include "ngsi/SubscriptionId.h"
#include "ngsi/RegistrationId.h"
#include "ngsi/StatusCode.h"

#include "mongoDriver/safeMongo.h"



/* ****************************************************************************
*
* orion::getObjectField -
*/
orion::BSONObj orion::getObjectField
(
  const orion::BSONObj&  _b,
  const std::string&     field,
  const std::string&     caller,
  int                    line
)
{
  // Getting the "low level" driver objects
  const mongo::BSONObj b = _b.get();

  if (b.hasField(field) && b.getField(field).type() == mongo::Object)
  {
    return orion::BSONObj(b.getObjectField(field));
  }

  // Detect error
  if (!b.hasField(field))
  {
    LM_E(("Runtime Error (object field '%s' is missing in BSONObj <%s> from caller %s:%d)",
          field.c_str(),
          b.toString().c_str(),
          caller.c_str(),
          line));
  }
  else
  {
    LM_E(("Runtime Error (field '%s' was supposed to be an object but type=%d in BSONObj <%s> from caller %s:%d)",
          field.c_str(), b.getField(field).type(), b.toString().c_str(), caller.c_str(), line));
  }
  return orion::BSONObj();
}



/* ****************************************************************************
*
* orion::getArrayField -
*/
orion::BSONArray orion::getArrayField
(
  const orion::BSONObj&  _b,
  const std::string&     field,
  const std::string&     caller,
  int                    line
)
{
  // Getting the "low level" driver objects
  const mongo::BSONObj b = _b.get();

  if (b.hasField(field) && b.getField(field).type() == mongo::Array)
  {
    // See http://stackoverflow.com/questions/36307126/getting-bsonarray-from-bsonelement-in-an-direct-way
    return orion::BSONArray((mongo::BSONArray) b.getObjectField(field));
  }

  // Detect error
  if (!b.hasField(field))
  {
    LM_E(("Runtime Error (object field '%s' is missing in BSONObj <%s> from caller %s:%d)",
          field.c_str(),
          b.toString().c_str(),
          caller.c_str(),
          line));
  }
  else
  {
    LM_E(("Runtime Error (field '%s' was supposed to be an array but type=%d in BSONObj <%s> from caller %s:%d)",
          field.c_str(), b.getField(field).type(), b.toString().c_str(), caller.c_str(), line));
  }
  return orion::BSONArray();
}



/* ****************************************************************************
*
* orion::getStringField -
*/
std::string orion::getStringField
(
  const orion::BSONObj&  _b,
  const std::string&     field,
  const std::string&     caller,
  int                    line
)
{
  // Getting the "low level" driver objects
  const mongo::BSONObj b = _b.get();

  if (b.hasField(field) && b.getField(field).type() == mongo::String)
  {
    return b.getStringField(field);
  }

  // Detect error
  if (!b.hasField(field))
  {
    LM_E(("Runtime Error (string field '%s' is missing in BSONObj <%s> from caller %s:%d)",
          field.c_str(),
          b.toString().c_str(),
          caller.c_str(),
          line));
  }
  else
  {
    LM_E(("Runtime Error (field '%s' was supposed to be a string but type=%d in BSONObj <%s> from caller %s:%d)",
          field.c_str(), b.getField(field).type(), b.toString().c_str(), caller.c_str(), line));
  }

  return "";
}



/* ****************************************************************************
*
* orion::getNumberField -
*/
double orion::getNumberField
(
  const orion::BSONObj&  _b,
  const std::string&     field,
  const std::string&     caller,
  int                    line
)
{
  // Getting the "low level" driver objects
  const mongo::BSONObj b = _b.get();

  if (b.hasField(field) && b.getField(field).type() == mongo::NumberDouble)
  {
    return b.getField(field).Number();
  }

  // Detect error
  if (!b.hasField(field))
  {
    LM_E(("Runtime Error (double field '%s' is missing in BSONObj <%s> from caller %s:%d)",
          field.c_str(),
          b.toString().c_str(),
          caller.c_str(),
          line));
  }
  else
  {
    LM_E(("Runtime Error (field '%s' was supposed to be an double but type=%d in BSONObj <%s> from caller %s:%d)",
          field.c_str(), b.getField(field).type(), b.toString().c_str(), caller.c_str(), line));
  }

  return -1;
}



/* ****************************************************************************
*
* orion::getIntField -
*/
int orion::getIntField
(
  const orion::BSONObj&  _b,
  const std::string&     field,
  const std::string&     caller,
  int                    line
)
{
  // Getting the "low level" driver objects
  const mongo::BSONObj b = _b.get();

  if (b.hasField(field) && b.getField(field).type() == mongo::NumberInt)
  {
    return b.getIntField(field);
  }

  // Detect error
  if (!b.hasField(field))
  {
    LM_E(("Runtime Error (int field '%s' is missing in BSONObj <%s> from caller %s:%d)",
          field.c_str(),
          b.toString().c_str(),
          caller.c_str(),
          line));
  }
  else
  {
    LM_E(("Runtime Error (field '%s' was supposed to be an int but type=%d in BSONObj <%s> from caller %s:%d)",
          field.c_str(), b.getField(field).type(), b.toString().c_str(), caller.c_str(), line));
  }

  return -1;
}



/* ****************************************************************************
*
* orion::getLongField -
*/
long long orion::getLongField
(
  const orion::BSONObj&  _b,
  const std::string&     field,
  const std::string&     caller,
  int                    line
)
{
  // Getting the "low level" driver objects
  const mongo::BSONObj b = _b.get();

  if (b.hasField(field) && (b.getField(field).type() == mongo::NumberLong))
  {
    return b.getField(field).Long();
  }

  // Detect error
  if (!b.hasField(field))
  {
    LM_E(("Runtime Error (long field '%s' is missing in BSONObj <%s> from caller %s:%d)",
          field.c_str(),
          b.toString().c_str(),
          caller.c_str(),
          line));
  }
  else
  {
    LM_E(("Runtime Error (field '%s' was supposed to be a long but type=%d in BSONObj <%s> from caller %s:%d)",
          field.c_str(), b.getField(field).type(), b.toString().c_str(), caller.c_str(), line));
  }

  return -1;
}



/* ****************************************************************************
*
* orion::getIntOrLongFieldAsLong -
*/
long long orion::getIntOrLongFieldAsLong
(
  const orion::BSONObj&  _b,
  const std::string&     field,
  const std::string&     caller,
  int                    line
)
{
  // Getting the "low level" driver objects
  const mongo::BSONObj b = _b.get();

  if (b.hasField(field))
  {
    if (b.getField(field).type() == mongo::NumberLong)
    {
      return b.getField(field).Long();
    }
    else if (b.getField(field).type() == mongo::NumberInt)
    {
      return b.getField(field).Int();
    }
  }

  // Detect error
  if (!b.hasField(field))
  {
    LM_E(("Runtime Error (int/long field '%s' is missing in BSONObj <%s> from caller %s:%d)",
          field.c_str(),
          b.toString().c_str(),
          caller.c_str(),
          line));
  }
  else
  {
    LM_E(("Runtime Error (field '%s' was supposed to be int or long but type=%d in BSONObj <%s> from caller %s:%d)",
          field.c_str(), b.getField(field).type(), b.toString().c_str(), caller.c_str(), line));
  }

  return -1;
}



/* ****************************************************************************
*
* orion::getBoolField -
*/
bool orion::getBoolField
(
  const orion::BSONObj&  _b,
  const std::string&     field,
  const std::string&     caller,
  int                    line
)
{
  // Getting the "low level" driver objects
  const mongo::BSONObj b = _b.get();

  if (b.hasField(field) && b.getField(field).type() == mongo::Bool)
  {
    return b.getBoolField(field);
  }

  // Detect error
  if (!b.hasField(field))
  {
    LM_E(("Runtime Error (bool field '%s' is missing in BSONObj <%s>)", field.c_str(), b.toString().c_str()));
  }
  else
  {
    field.c_str();
    b.getField(field);
    b.getField(field).type();
    b.toString();
    b.toString().c_str();
    LM_E(("Runtime Error (field '%s' was supposed to be a bool but type=%d in BSONObj <%s> from caller %s:%d)",
          field.c_str(), b.getField(field).type(), b.toString().c_str(), caller.c_str(), line));
  }

  return false;
}



/* ****************************************************************************
*
* getField -
*/
orion::BSONElement orion::getField
(
  const orion::BSONObj&  _b,
  const std::string&     field,
  const std::string&     caller,
  int                    line
)
{
  // Getting the "low level" driver objects
  const mongo::BSONObj b = _b.get();

  if (b.hasField(field))
  {
    return orion::BSONElement(b.getField(field));
  }

  LM_E(("Runtime Error (field '%s' is missing in BSONObj <%s> from caller %s:%d)",
        field.c_str(),
        b.toString().c_str(),
        caller.c_str(),
        line));

  return orion::BSONElement();
}



/* ****************************************************************************
*
* setStringVector -
*/
void orion::setStringVector
(
  const orion::BSONObj&      _b,
  const std::string&         field,
  std::vector<std::string>*  v,
  const std::string&         caller,
  int                        line
)
{
  // Getting the "low level" driver objects
  const mongo::BSONObj b = _b.get();

  if (b.hasField(field) && b.getField(field).type() == mongo::Array)
  {
    // See http://stackoverflow.com/questions/36307126/getting-bsonarray-from-bsonelement-in-an-direct-way
    std::vector<mongo::BSONElement> ba = b.getField(field).Array();

    v->clear();

    for (unsigned int ix = 0; ix < ba.size(); ++ix)
    {
      if (ba[ix].type() == mongo::String)
      {
        v->push_back(ba[ix].String());
      }
      else
      {
        LM_E(("Runtime Error (element %d in array was supposed to be an string but type=%d from caller %s:%d)",
              ix, ba[ix].type(), caller.c_str(), line));
        v->clear();

        return;
      }
    }
  }
  else
  {
    // Detect error
    if (!b.hasField(field))
    {
      LM_E(("Runtime Error (object field '%s' is missing in BSONObj <%s> from caller %s:%d)",
            field.c_str(),
            b.toString().c_str(),
            caller.c_str(),
            line));
    }
    else
    {
      LM_E(("Runtime Error (field '%s' was supposed to be an array but type=%d in BSONObj <%s> from caller %s:%d)",
            field.c_str(), b.getField(field).type(), b.toString().c_str(), caller.c_str(), line));
    }
  }
}



/* ****************************************************************************
*
* orion::moreSafe -
*
* This is a safe version of the more() method for cursors in order to avoid
* exception that may crash the broker. However, if the more() method is returning
* an exception something really bad is happening, it is considered a Fatal Error.
*
* (The name resembles the same relationship between next() and nextSafe() in the mongo driver)
*/
bool orion::moreSafe(orion::DBCursor* cursor)
{
  try
  {
    return cursor->more();
  }
  catch (const std::exception& e)
  {
    LM_E(("Fatal Error (more() exception: %s)", e.what()));
    return false;
  }
  catch (...)
  {
    LM_E(("Fatal Error (more() exception: generic)"));
    return false;
  }
}



/* ****************************************************************************
*
* orion::nextSafeOrError -
*/
bool orion::nextSafeOrError
(
  orion::DBCursor&    cursor,
  orion::BSONObj*     r,
  std::string*        err,
  const std::string&  caller,
  int                 line
)
{
  try
  {
    *r = cursor.nextSafe();
    return true;
  }
  catch (const std::exception &e)
  {
    char lineString[STRING_SIZE_FOR_INT];

    snprintf(lineString, sizeof(lineString), "%d", line);
    *err = std::string(e.what()) + " at " + caller + ":" + lineString;

    return false;
  }
  catch (...)
  {
    char lineString[STRING_SIZE_FOR_INT];

    snprintf(lineString, sizeof(lineString), "%d", line);
    *err = "generic exception at " + caller + ":" + lineString;

    return false;
  }
}



/* ****************************************************************************
*
* orion::safeGetSubId -
*/
bool orion::safeGetSubId(const SubscriptionId& subId, orion::OID* id, StatusCode* sc)
{
  try
  {
    *id = orion::OID(mongo::OID(subId.get()));
    return true;
  }
  catch (const mongo::AssertionException &e)
  {
    // FIXME P3: this check is "defensive", but from a efficiency perspective this should be short-cut at
    // parsing stage. To check it. The check should be for: [0-9a-fA-F]{24}.
    sc->fill(SccContextElementNotFound);
    return false;
  }
  catch (const std::exception &e)
  {
    sc->fill(SccReceiverInternalError, e.what());
    return false;
  }
  catch (...)
  {
    sc->fill(SccReceiverInternalError, "generic exception");
    return false;
  }
}



/* ****************************************************************************
*
* orion::safeGetRegId -
*/
bool orion::safeGetRegId(const RegistrationId& regId, orion::OID* id, StatusCode* sc)
{
  try
  {
    *id = orion::OID(mongo::OID(regId.get()));
    return true;
  }
  catch (const mongo::AssertionException &e)
  {
    //
    // FIXME P3: this check is "defensive", but from a efficiency perspective this should be short-cut at
    // parsing stage. To check it. The check should be for: [0-9a-fA-F]{24}.
    //
    sc->fill(SccContextElementNotFound);
    return false;
  }
  catch (const std::exception &e)
  {
    sc->fill(SccReceiverInternalError, e.what());
    return false;
  }
  catch (...)
  {
    sc->fill(SccReceiverInternalError, "generic exception");
    return false;
  }
}
