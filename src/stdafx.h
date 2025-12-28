#ifndef STDAFX_H
#define STDAFX_H

#ifdef _WINDOWS

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

#endif //_WINDOWS

#endif //STDAFX_H
