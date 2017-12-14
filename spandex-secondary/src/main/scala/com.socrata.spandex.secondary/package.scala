package com.socrata.spandex

import com.socrata.datacoordinator.{secondary => DCSecondary}
import com.socrata.soql.types.{SoQLValue, SoQLType}

package object secondary {
  type Event = DCSecondary.Event[SoQLType, SoQLValue]
  type Operation = DCSecondary.Operation[SoQLValue]
  type Insert = DCSecondary.Insert[SoQLValue]
  type Update = DCSecondary.Update[SoQLValue]
  type Delete = DCSecondary.Delete[SoQLValue]
}
